import https from 'https';
import zlib from 'zlib';
import { randomUUID } from 'crypto';

const ACCOUNT = process.env.SNOWFLAKE_ACCOUNT!;
const USER = process.env.SNOWFLAKE_USER!;
const PASSWORD = process.env.SNOWFLAKE_PASSWORD!;
const WAREHOUSE = process.env.SNOWFLAKE_WAREHOUSE!;
const DATABASE = process.env.SNOWFLAKE_DATABASE!;
const HOST = `${ACCOUNT}.snowflakecomputing.com`;

function httpsPost(path: string, body: object, headers: Record<string, string>): Promise<any> {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(body);
    const options = {
      hostname: HOST,
      port: 443,
      path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr),
        ...headers,
      },
    };
    const req = https.request(options, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString()));
        } catch (e) {
          reject(new Error(`Failed to parse response: ${Buffer.concat(chunks).toString().slice(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

function fetchChunk(url: string, headers: Record<string, string> = {}): Promise<string[][]> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const options = {
      hostname: parsed.hostname,
      port: 443,
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers,
    };
    const req = https.request(options, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        zlib.gunzip(buf, (err, decompressed) => {
          const text = err ? buf.toString() : decompressed.toString();
          try {
            // Format is newline-delimited rows: ["val1","val2",...],\n["val1",...]
            // Wrap in array brackets and parse
            const rows = JSON.parse('[' + text.replace(/,\s*$/, '') + ']');
            resolve(rows);
          } catch (e) {
            reject(new Error(`Failed to parse chunk: ${text.slice(0, 200)}`));
          }
        });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getToken(): Promise<string> {
  const creds = Buffer.from(`${USER}:${PASSWORD}`).toString('base64');
  const path = `/session/v1/login-request?warehouse=${WAREHOUSE}&databaseName=${DATABASE}&schemaName=PUBLIC`;
  const body = {
    data: {
      CLIENT_APP_ID: 'nextjs-dashboard',
      CLIENT_APP_VERSION: '1.0',
      SVN_REVISION: '1',
      ACCOUNT_NAME: ACCOUNT,
      LOGIN_NAME: USER,
      PASSWORD,
      CLIENT_ENVIRONMENT: {
        APPLICATION: 'nextjs-dashboard',
        OS: 'linux',
        OS_VERSION: 'linux',
        OCSP_MODE: 'FAIL_OPEN',
      },
    },
  };
  const res = await httpsPost(path, body, {
    Authorization: `Basic ${creds}`,
    'X-Snowflake-Authorization-Token-Type': 'BASIC',
  });
  if (!res.success) throw new Error(`Snowflake login failed: ${JSON.stringify(res)}`);
  return res.data.token;
}

function convertValue(value: string, colType: string): any {
  if (value === null || value === undefined) return null;
  if (colType === 'timestamp_tz' || colType === 'timestamp_ltz' || colType === 'timestamp_ntz') {
    const epochSeconds = parseFloat(value.split(' ')[0]);
    return new Date(epochSeconds * 1000).toISOString();
  }
  if (colType === 'real') return parseFloat(value);
  return value;
}

export async function querySnowflake(sql: string, _binds: any[] = []): Promise<any[]> {
  const token = await getToken();
  const path = `/queries/v1/query-request?requestId=${randomUUID()}`;
  const body = {
    sqlText: sql,
    asyncExec: false,
    sequenceId: 1,
    querySubmissionTime: Date.now(),
  };
  const res = await httpsPost(path, body, {
    Authorization: `Snowflake Token="${token}"`,
    'X-Snowflake-Authorization-Token-Type': 'SESSION',
  });

  if (!res.success) throw new Error(`Snowflake query failed: ${JSON.stringify(res.message)}`);

  const { rowtype, rowset, chunks, chunkHeaders } = res.data;
  let allRows: string[][] = rowset || [];

  if (chunks && chunks.length > 0) {
    const chunkData = await Promise.all(
      chunks.map((chunk: { url: string }) => fetchChunk(chunk.url, chunkHeaders || {}))
    );
    for (const chunk of chunkData) {
      if (Array.isArray(chunk)) allRows = allRows.concat(chunk);
    }
  }

  if (allRows.length === 0) return [];

  return allRows.map((row: string[]) =>
    Object.fromEntries(
      rowtype.map((col: any, i: number) => [
        col.name.toLowerCase(),
        convertValue(row[i], col.type),
      ])
    )
  );
}
