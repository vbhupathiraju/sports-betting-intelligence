import { NextRequest, NextResponse } from 'next/server';
import { querySnowflake } from '@/lib/snowflake';

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const home = searchParams.get('home_team');
  const away = searchParams.get('away_team');
  if (!home || !away) return NextResponse.json([]);

  const sql = `
    SELECT market_ticker, computed_at, kalshi_implied_prob,
           sportsbook_home_prob, divergence
    FROM SPORTS_BETTING.PUBLIC.divergence_signals
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
