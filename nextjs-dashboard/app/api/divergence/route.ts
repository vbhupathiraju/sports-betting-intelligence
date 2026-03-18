import { NextRequest, NextResponse } from 'next/server';
import { querySnowflake } from '@/lib/snowflake';

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const date = searchParams.get('date');
  const sport = searchParams.get('sport');

  let sql = 'SELECT * FROM SPORTS_BETTING.PUBLIC.v_divergence_latest WHERE 1=1';
  const binds: any[] = [];

  if (date) { sql += ` AND DATE(commence_time) = '${date}'`; }
  if (sport && sport !== 'all') { sql += ` AND sport_key = '${sport}'`; }
  sql += ' ORDER BY divergence DESC NULLS LAST';

  try {
    const rows = await querySnowflake(sql, binds);
    return NextResponse.json(rows);
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
