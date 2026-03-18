const SNOWFLAKE_ACCOUNT = process.env.SNOWFLAKE_ACCOUNT!;
const SNOWFLAKE_USER = process.env.SNOWFLAKE_USER!;
const SNOWFLAKE_PASSWORD = process.env.SNOWFLAKE_PASSWORD!;
const SNOWFLAKE_DATABASE = process.env.SNOWFLAKE_DATABASE!;
const SNOWFLAKE_WAREHOUSE = process.env.SNOWFLAKE_WAREHOUSE!;

const BASE_URL = `https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com`;

async function getToken(): Promise<string> {
  const creds = Buffer.from(`${SNOWFLAKE_USER}:${SNOWFLAKE_PASSWORD}`).toString('base64');
  const res = await fetch(`${BASE_URL}/session/v1/login-request?warehouse=${SNOWFLAKE_WAREHOUSE}&databaseName=${SNOWFLAKE_DATABASE}&schemaName=PUBLIC`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Basic ${creds}`,
      'X-Snowflake-Authorization-Token-Type': 'BASIC',
    },
    body: JSON.stringify({
      data: {
        CLIENT_APP_ID: 'nextjs-dashboard',
        CLIENT_APP_VERSION: '1.0',
        SVN_REVISION: '1',
        ACCOUNT_NAME: SNOWFLAKE_ACCOUNT,
        LOGIN_NAME: SNOWFLAKE_USER,
        PASSWORD: SNOWFLAKE_PASSWORD,
        CLIENT_ENVIRONMENT: {
          APPLICATION: 'nextjs-dashboard',
          OS: 'linux',
          OS_VERSION: 'linux',
          OCSP_MODE: 'FAIL_OPEN',
        },
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Snowflake login failed: ${res.status} ${text}`);
  }

  const json = await res.json();
  if (!json.success) throw new Error(`Snowflake login error: ${JSON.stringify(json)}`);
  return json.data.token;
}

export async function querySnowflake(sql: string, _binds: any[] = []): Promise<any[]> {
  const token = await getToken();

  const res = await fetch(`${BASE_URL}/queries/v1/query-request?requestId=${crypto.randomUUID()}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Snowflake Token="${token}"`,
      'X-Snowflake-Authorization-Token-Type': 'SESSION',
    },
    body: JSON.stringify({
      sqlText: sql,
      asyncExec: false,
      sequenceId: 1,
      querySubmissionTime: Date.now(),
      parameters: {
        QUERY_TAG: 'nextjs-dashboard',
      },
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Snowflake query failed: ${res.status} ${text}`);
  }

  const json = await res.json();
  if (!json.success) throw new Error(`Snowflake query error: ${JSON.stringify(json.message)}`);

  const { rowtype, rowset } = json.data;
  if (!rowset || rowset.length === 0) return [];

  // Map column names to values
  return rowset.map((row: string[]) =>
    Object.fromEntries(rowtype.map((col: any, i: number) => [col.name.toLowerCase(), row[i]]))
  );
}
