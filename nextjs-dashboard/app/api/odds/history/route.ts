import { NextRequest, NextResponse } from 'next/server';
import { querySnowflake } from '@/lib/snowflake';

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const home = searchParams.get('home_team');
  const away = searchParams.get('away_team');
  if (!home || !away) return NextResponse.json([]);

  const sql = `
    SELECT team, bookmaker_key, computed_at, american_odds,
           prev_implied_prob, current_implied_prob
    FROM SPORTS_BETTING.PUBLIC.sharp_money_signals
    WHERE home_team = '${home}' AND away_team = '${away}'
    ORDER BY computed_at ASC
  `;
  try {
    const rows = await querySnowflake(sql);
    return NextResponse.json(rows);
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
