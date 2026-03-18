import snowflake from 'snowflake-sdk';

const getConnection = () => snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT!,
  username: process.env.SNOWFLAKE_USER!,
  password: process.env.SNOWFLAKE_PASSWORD!,
  database: process.env.SNOWFLAKE_DATABASE!,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
  schema: 'PUBLIC',
});

export async function querySnowflake(sql: string, binds: any[] = []): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const conn = getConnection();
    conn.connect((err) => {
      if (err) return reject(err);
      conn.execute({
        sqlText: sql,
        binds,
        complete: (err, _stmt, rows) => {
          conn.destroy(() => {});
          if (err) return reject(err);
          const lower = (rows || []).map((r: any) =>
            Object.fromEntries(Object.entries(r).map(([k, v]) => [k.toLowerCase(), v]))
          );
          resolve(lower);
        }
      });
    });
  });
}
