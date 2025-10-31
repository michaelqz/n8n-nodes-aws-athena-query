import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IHttpRequestOptions,
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError, NodeApiError } from 'n8n-workflow';
import { createHash, createHmac } from 'crypto';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// AWS Signature V4 implementation
class AWSSignatureV4 {
	private accessKeyId: string;
	private secretAccessKey: string;
	private sessionToken?: string;
	private region: string;
	private service: string;

	constructor(
		accessKeyId: string,
		secretAccessKey: string,
		region: string,
		service: string,
		sessionToken?: string,
	) {
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.sessionToken = sessionToken;
		this.region = region;
		this.service = service;
	}

	private hash(data: string): string {
		return createHash('sha256').update(data, 'utf8').digest('hex');
	}

	private hmac(key: string | Buffer, data: string): Buffer {
		return createHmac('sha256', key).update(data, 'utf8').digest();
	}

	private getSignatureKey(dateStamp: string): Buffer {
		const kDate = this.hmac('AWS4' + this.secretAccessKey, dateStamp);
		const kRegion = this.hmac(kDate, this.region);
		const kService = this.hmac(kRegion, this.service);
		const kSigning = this.hmac(kService, 'aws4_request');
		return kSigning;
	}

	sign(
		method: string,
		url: string,
		headers: Record<string, string>,
		payload: string,
	): Record<string, string> {
		const urlObj = new URL(url);
		const pathname = urlObj.pathname;
		const querystring = urlObj.search.slice(1);

		const now = new Date();
		const amzDate = now.toISOString().replace(/[:\-]|\.\d{3}/g, '');
		const dateStamp = amzDate.slice(0, 8);

		// Canonical headers
		const signedHeadersNames = Object.keys(headers)
			.map((key) => key.toLowerCase())
			.sort()
			.join(';');

		const canonicalHeaders =
			Object.keys(headers)
				.sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()))
				.map((key) => `${key.toLowerCase()}:${headers[key]}`)
				.join('\n') + '\n';

		// Canonical request
		const payloadHash = this.hash(payload);
		const canonicalRequest = [
			method,
			pathname,
			querystring,
			canonicalHeaders,
			signedHeadersNames,
			payloadHash,
		].join('\n');

		// String to sign
		const algorithm = 'AWS4-HMAC-SHA256';
		const credentialScope = `${dateStamp}/${this.region}/${this.service}/aws4_request`;
		const stringToSign = [algorithm, amzDate, credentialScope, this.hash(canonicalRequest)].join(
			'\n',
		);

		// Calculate signature
		const signingKey = this.getSignatureKey(dateStamp);
		const signature = this.hmac(signingKey, stringToSign).toString('hex');

		// Authorization header
		const authorizationHeader = `${algorithm} Credential=${this.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeadersNames}, Signature=${signature}`;

		const resultHeaders: Record<string, string> = {
			...headers,
			Authorization: authorizationHeader,
			'X-Amz-Date': amzDate,
		};

		if (this.sessionToken) {
			resultHeaders['X-Amz-Security-Token'] = this.sessionToken;
		}

		return resultHeaders;
	}
}

// Helper function to make Athena API requests
async function makeAthenaRequest(
	executeFunctions: IExecuteFunctions,
	region: string,
	target: string,
	payload: any,
	credentials: any,
): Promise<any> {
	const endpoint = `https://athena.${region}.amazonaws.com/`;
	const payloadString = JSON.stringify(payload);

	const headers = {
		'Content-Type': 'application/x-amz-json-1.1',
		'X-Amz-Target': target,
		Host: `athena.${region}.amazonaws.com`,
	};

	const signer = new AWSSignatureV4(
		credentials.accessKeyId as string,
		credentials.secretAccessKey as string,
		region,
		'athena',
		credentials.sessionToken as string | undefined,
	);

	const signedHeaders = signer.sign('POST', endpoint, headers, payloadString);

	const options: IHttpRequestOptions = {
		method: 'POST',
		url: endpoint,
		headers: signedHeaders,
		body: payloadString,
		json: true,
	};

	try {
		const response = await executeFunctions.helpers.httpRequest(options);
		return response;
	} catch (error: any) {
		// Enhanced error handling for AWS API responses
		let errorMessage = 'AWS Athena API request failed';
		let statusCode = 'Unknown';
		let awsErrorCode = 'Unknown';
		let awsErrorMessage = 'Unknown';
		let responseBody = 'No response body';

		if (error.response) {
			statusCode = error.response.statusCode || error.response.status || 'Unknown';
			responseBody = error.response.body || error.response.data || 'No response body';

			// Try to parse AWS error response in multiple formats
			try {
				let errorBodyStr = '';
				if (typeof responseBody === 'string') {
					errorBodyStr = responseBody;
				} else if (typeof responseBody === 'object') {
					errorBodyStr = JSON.stringify(responseBody);
				} else {
					errorBodyStr = String(responseBody);
				}

				if (errorBodyStr) {
					// Try parsing as JSON first
					try {
						const awsError = JSON.parse(errorBodyStr);
						awsErrorCode = awsError.__type || awsError.Code || awsError.code || 'Unknown';
						awsErrorMessage = awsError.message || awsError.Message || awsError.msg || errorBodyStr;
					} catch (jsonParseError) {
						// If JSON parsing fails, check if it's an XML response
						if (errorBodyStr.includes('<')) {
							// Basic XML parsing for AWS errors
							const codeMatch = errorBodyStr.match(/<Code>([^<]+)<\/Code>/);
							const messageMatch = errorBodyStr.match(/<Message>([^<]+)<\/Message>/);
							awsErrorCode = codeMatch ? codeMatch[1] : 'XMLParseError';
							awsErrorMessage = messageMatch ? messageMatch[1] : errorBodyStr;
						} else {
							awsErrorMessage = errorBodyStr;
						}
					}
				}
			} catch (parseError) {
				awsErrorMessage = `Parse error: ${parseError.message}`;
			}

			errorMessage = `AWS Athena API Error (${statusCode}): ${awsErrorCode} - ${awsErrorMessage}`;
		} else if (error.message) {
			errorMessage = `Request Error: ${error.message}`;
		}

		// Add comprehensive debugging information
		const debugInfo = {
			endpoint,
			target,
			payload: payload,
			region,
			statusCode,
			awsErrorCode,
			awsErrorMessage,
			responseBody,
			requestHeaders: signedHeaders,
			originalError: error.message,
			errorType: error.constructor.name,
		};

		if (error.response) {
			// This is an API error from AWS - use NodeApiError
			throw new NodeApiError(executeFunctions.getNode(), error, {
				message: `${errorMessage}\nDebug Info: ${JSON.stringify(debugInfo, null, 2)}`,
			});
		} else {
			// This is a general operation error - use NodeOperationError
			throw new NodeOperationError(
				executeFunctions.getNode(),
				`${errorMessage}\nDebug Info: ${JSON.stringify(debugInfo, null, 2)}`,
			);
		}
	}
}

export class AwsAthenaQuery implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'AWS Athena Query',
		name: 'awsAthenaQuery',
		icon: 'file:AwsAthenaQuery.node.svg',
		group: ['transform'],
		version: 1,
		description: 'Execute SQL queries on AWS Athena',
		defaults: {
			name: 'AWS Athena Query',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		usableAsTool: true,
		credentials: [
			{
				name: 'aws',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Region',
				name: 'region',
				type: 'string',
				default: 'us-east-1',
				placeholder: 'us-east-1',
				description: 'AWS region where your Athena service is located',
				required: true,
			},
			{
				displayName: 'Database Name',
				name: 'database',
				type: 'string',
				default: '',
				placeholder: 'Optional',
				description: 'Name of the database to query. Leave empty to use the default database.',
			},
			{
				displayName: 'Workgroup',
				name: 'workgroup',
				type: 'string',
				default: '',
				placeholder: 'primary',
				description:
					'Name of the Athena workgroup to use. Leave empty to use the default workgroup (primary).',
			},
			{
				displayName: 'SQL Query',
				name: 'query',
				type: 'string',
				default: '',
				noDataExpression: false,
				required: true,
				typeOptions: {
					editor: 'sqlEditor',
					rows: 5,
				},
				placeholder: 'SELECT * FROM my_table LIMIT 10',
				description: 'The SQL query to execute',
			},
			{
				displayName: 'S3 Output Location',
				name: 's3OutputLocation',
				type: 'string',
				default: '',
				placeholder: 's3://my-bucket/athena-results/',
				description: 'S3 bucket path where Athena will save query results',
				required: true,
			},
			{
				displayName: 'Query Timeout (Seconds)',
				name: 'timeout',
				type: 'number',
				default: 300,
				description: 'Maximum time to wait for query completion. Defaults to 300 seconds.',
				required: true,
			},
			{
				displayName: 'Output Format',
				name: 'outputFormat',
				type: 'options',
				options: [
					{
						name: 'Table Format',
						value: 'tableFormat',
						description:
							'Each database row becomes a separate workflow item (best for data processing)',
					},
					{
						name: 'Raw Format',
						value: 'rawFormat',
						description:
							'All results in one item with additional metadata (query ID, row count, etc.)',
					},
				],
				default: 'tableFormat',
				description: 'How to structure the query results for use in your workflow',
				required: true,
			},
			{
				displayName: 'Max Rows Returned',
				name: 'maxRowsMode',
				type: 'options',
				options: [
					{
						name: 'No Limit',
						value: 'noLimit',
						description: 'Return all available rows (Warning: May be slow)',
					},
					{
						name: 'Limit Applied',
						value: 'limitApplied',
						description: 'Return up to a maximum number of rows',
					},
				],
				default: 'noLimit',
				description: 'Control how many rows are returned from the query',
				required: true,
			},
			{
				displayName: 'Max Rows',
				name: 'maxRows',
				type: 'number',
				default: 10000,
				description: 'Maximum number of rows to return when limit is applied',
				required: true,
				displayOptions: {
					show: {
						maxRowsMode: ['limitApplied'],
					},
				},
				typeOptions: {
					minValue: 1,
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const resultItems: INodeExecutionData[] = [];

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				// Get node parameters
				const region = this.getNodeParameter('region', itemIndex) as string;
				const database = this.getNodeParameter('database', itemIndex) as string;
				const workgroup = this.getNodeParameter('workgroup', itemIndex) as string;
				const query = this.getNodeParameter('query', itemIndex) as string;
				const s3OutputLocation = this.getNodeParameter('s3OutputLocation', itemIndex) as string;
				const outputFormat = this.getNodeParameter(
					'outputFormat',
					itemIndex,
					'tableFormat',
				) as string;
				const timeout = this.getNodeParameter('timeout', itemIndex, 300) as number;

				// Validate required parameters
				if (!region || !region.trim()) {
					throw new NodeOperationError(this.getNode(), 'Region is required.');
				}
				if (!query || !query.trim()) {
					throw new NodeOperationError(this.getNode(), 'SQL Query is required.');
				}
				if (!s3OutputLocation || !s3OutputLocation.trim()) {
					throw new NodeOperationError(this.getNode(), 'S3 Output Location is required.');
				}
				if (timeout <= 0) {
					throw new NodeOperationError(this.getNode(), 'Timeout must be greater than 0.');
				}

				// Get AWS credentials
				const credentials = await this.getCredentials('aws');

				if (!credentials.accessKeyId || !credentials.secretAccessKey) {
					throw new NodeOperationError(
						this.getNode(),
						'Invalid AWS credentials. Please ensure they are configured correctly.',
					);
				}

				// Generate a unique client request token for idempotency (minimum 32 characters required)
				const timestamp = Date.now().toString();
				const randomPart =
					Math.random().toString(36).substring(2, 17) + Math.random().toString(36).substring(2, 17);
				const clientRequestToken = `n8n-${timestamp}-${randomPart}`.substring(0, 64); // Ensure it's at least 32 chars, max 64

				// Prepare query execution parameters
				const queryParams: any = {
					QueryString: query,
					ClientRequestToken: clientRequestToken,
					ResultConfiguration: {
						OutputLocation: s3OutputLocation,
					},
				};

				// Add database context if provided
				if (database && database.trim() !== '') {
					queryParams.QueryExecutionContext = {
						Database: database,
					};
				}

				// Add workgroup if provided
				if (workgroup && workgroup.trim() !== '') {
					queryParams.WorkGroup = workgroup;
				}

				// Read row limit settings
				const maxRowsMode = this.getNodeParameter('maxRowsMode', itemIndex, 'noLimit') as string;
				const maxRowsValue =
					maxRowsMode === 'limitApplied'
						? (this.getNodeParameter('maxRows', itemIndex) as number)
						: undefined;

				if (maxRowsMode === 'limitApplied') {
					if (!Number.isInteger(maxRowsValue) || (maxRowsValue as number) <= 0) {
						throw new NodeOperationError(this.getNode(), 'Max Rows must be a positive integer.');
					}
				}

				// Start query execution
				const startResponse = await makeAthenaRequest(
					this,
					region,
					'AmazonAthena.StartQueryExecution',
					queryParams,
					credentials,
				);

				const queryExecutionId = startResponse.QueryExecutionId;

				if (!queryExecutionId) {
					throw new NodeOperationError(this.getNode(), 'Failed to start Athena query execution.');
				}

				// Wait for query completion
				let queryStatus = 'RUNNING';
				const startTime = Date.now();
				const timeoutMs = timeout * 1000;

				while (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
					// Check timeout
					if (Date.now() - startTime > timeoutMs) {
						throw new NodeOperationError(
							this.getNode(),
							`Query timed out after ${timeout} seconds.`,
						);
					}

					// Wait before checking status again
					await sleep(2000); // Wait 2 seconds

					const statusResponse = await makeAthenaRequest(
						this,
						region,
						'AmazonAthena.GetQueryExecution',
						{ QueryExecutionId: queryExecutionId },
						credentials,
					);

					queryStatus = statusResponse.QueryExecution?.Status?.State || 'FAILED';

					if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
						const reason =
							statusResponse.QueryExecution?.Status?.StateChangeReason || 'Unknown error';
						throw new NodeOperationError(
							this.getNode(),
							`Query failed or was cancelled: ${reason}`,
						);
					}

					// Handle unexpected query states
					if (
						queryStatus !== 'RUNNING' &&
						queryStatus !== 'QUEUED' &&
						queryStatus !== 'SUCCEEDED'
					) {
						throw new NodeOperationError(
							this.getNode(),
							`Query ended with unexpected status: ${queryStatus}`,
						);
					}
				}

				// Get query results with pagination (Athena returns up to 1000 rows per page including a header row on the first page)
				let nextToken: string | undefined = undefined;
				let pageIndex = 0;
				let columns: string[] = [];
				const parsedResults: Array<{ [key: string]: any }> = [];

				do {
					const payload: any = { QueryExecutionId: queryExecutionId, MaxResults: 1000 };
					if (nextToken) payload.NextToken = nextToken;

					const resultsResponse = await makeAthenaRequest(
						this,
						region,
						'AmazonAthena.GetQueryResults',
						payload,
						credentials,
					);

					const rows = resultsResponse.ResultSet?.Rows || [];

					if (rows.length === 0) {
						nextToken = resultsResponse.NextToken;
						pageIndex += 1;
						continue;
					}

					// On first page, extract header row as column names
					let dataRows: any[] = rows;
					if (pageIndex === 0) {
						columns = rows[0]?.Data?.map((data: any) => data.VarCharValue || '') || [];
						dataRows = rows.slice(1); // skip header on first page
					}

					for (const row of dataRows) {
						const rowData = row.Data || [];
						const parsedRow: { [key: string]: any } = {};

						columns.forEach((column: any, index: number) => {
							if (column) {
								const cellData = rowData[index];
								let value = null;
								if (cellData) {
									value =
										cellData.VarCharValue ||
										cellData.BigIntValue ||
										cellData.BooleanValue ||
										cellData.DateValue ||
										cellData.DoubleValue ||
										cellData.FloatValue ||
										cellData.IntegerValue ||
										cellData.TimestampValue ||
										null;
								}
								parsedRow[column] = value;
							}
						});

						parsedResults.push(parsedRow);
						if (
							maxRowsMode === 'limitApplied' &&
							parsedResults.length >= (maxRowsValue as number)
						) {
							// Stop collecting more rows; break out after current page
							break;
						}
					}

					// If we've reached the max rows, stop pagination
					if (maxRowsMode === 'limitApplied' && parsedResults.length >= (maxRowsValue as number)) {
						nextToken = undefined;
					} else {
						nextToken = resultsResponse.NextToken;
					}
					pageIndex += 1;
				} while (nextToken);

				// If no data rows were returned
				if (parsedResults.length === 0) {
					if (outputFormat === 'rawFormat') {
						resultItems.push({
							json: {
								queryExecutionId,
								rowCount: 0,
								columns: [],
								results: [],
							},
						});
					}
					continue;
				}

				// Add results to output based on format
				if (outputFormat === 'tableFormat') {
					for (const row of parsedResults) {
						resultItems.push({ json: row });
					}
				} else {
					resultItems.push({
						json: {
							queryExecutionId,
							rowCount: parsedResults.length,
							columns,
							results: parsedResults,
						},
					});
				}
			} catch (error: any) {
				if (this.continueOnFail()) {
					resultItems.push({
						json: this.getInputData(itemIndex)[0].json,
						error,
						pairedItem: itemIndex,
					});
				} else {
					throw new NodeOperationError(this.getNode(), error, {
						itemIndex,
					});
				}
			}
		}

		return [resultItems];
	}
}
