import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionType, NodeOperationError } from 'n8n-workflow';
import {
	AthenaClient,
	StartQueryExecutionCommand,
	GetQueryExecutionCommand,
	GetQueryResultsCommand,
	type StartQueryExecutionCommandInput,
} from '@aws-sdk/client-athena';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

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
						description: 'Each database row becomes a separate workflow item (best for data processing)',
					},
					{
						name: 'Raw Format',
						value: 'rawFormat',
						description: 'All results in one item with additional metadata (query ID, row count, etc.)',
					},
				],
				default: 'tableFormat',
				description: 'How to structure the query results for use in your workflow',
				required: true,
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
				const query = this.getNodeParameter('query', itemIndex) as string;
				const s3OutputLocation = this.getNodeParameter('s3OutputLocation', itemIndex) as string;
				const outputFormat = this.getNodeParameter('outputFormat', itemIndex, 'tableFormat') as string;
				const timeout = this.getNodeParameter('timeout', itemIndex, 300) as number;

				// Get AWS credentials
				const credentials = await this.getCredentials('aws');

				if (!credentials.accessKeyId || !credentials.secretAccessKey) {
					throw new NodeOperationError(
						this.getNode(),
						'Invalid AWS credentials. Please ensure they are configured correctly.',
					);
				}

				// Configure AWS Athena client (v3)
				const clientConfig: any = {
					region,
					credentials: {
						accessKeyId: credentials.accessKeyId as string,
						secretAccessKey: credentials.secretAccessKey as string,
					},
				};

				// Add session token if available (for temporary credentials)
				if (credentials.sessionToken) {
					clientConfig.credentials.sessionToken = credentials.sessionToken as string;
				}

				const athenaClient = new AthenaClient(clientConfig);

				// Prepare query execution parameters
				const queryParams: StartQueryExecutionCommandInput = {
					QueryString: query,
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

				// Start query execution
				const startCommand = new StartQueryExecutionCommand(queryParams);
				const startResponse = await athenaClient.send(startCommand);
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

					const statusCommand = new GetQueryExecutionCommand({
						QueryExecutionId: queryExecutionId,
					});
					const statusResponse = await athenaClient.send(statusCommand);

					queryStatus = statusResponse.QueryExecution?.Status?.State || 'FAILED';

					if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
						const reason = statusResponse.QueryExecution?.Status?.StateChangeReason || 'Unknown error';
						throw new NodeOperationError(
							this.getNode(),
							`Query failed or was cancelled: ${reason}`,
						);
					}
				}

				// Get query results
				const resultsCommand = new GetQueryResultsCommand({
					QueryExecutionId: queryExecutionId,
				});
				const resultsResponse = await athenaClient.send(resultsCommand);

				const rows = resultsResponse.ResultSet?.Rows || [];
				
				if (rows.length === 0) {
					// No results returned
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
					// For tableFormat with no results, don't add any items
					continue;
				}

				// Extract column names from the first row
				const columns = rows[0]?.Data?.map((data: any) => data.VarCharValue || '') || [];

				// Parse data rows (skip header row)
				const parsedResults = rows.slice(1).map((row: any) => {
					const rowData = row.Data || [];
					const parsedRow: { [key: string]: string | null } = {};
					
					columns.forEach((column: any, index: number) => {
						if (column) {
							parsedRow[column] = rowData[index]?.VarCharValue || null;
						}
					});
					
					return parsedRow;
				});

				// Add results to output based on format
				if (outputFormat === 'tableFormat') {
					// Table format: each row becomes a separate item
					parsedResults.forEach((row) => {
						resultItems.push({
							json: row,
						});
					});
				} else {
					// Raw format: return all data in one item with metadata
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
