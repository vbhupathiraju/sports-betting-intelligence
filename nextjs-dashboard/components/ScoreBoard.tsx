'use client';

interface ScoreData {
  home_team: string;
  away_team: string;
  home_score: string;
  away_score: string;
  status: string;
  state: string;
  period: number;
  display_clock: string;
  commence_time: string;
}

function periodLabel(sportKey: string, period: number) {
  if (period === 0) return '';
  if (sportKey === 'basketball_nba') return period <= 4 ? `Q${period}` : `OT${period - 4}`;
  if (sportKey === 'basketball_ncaab') return period <= 2 ? `H${period}` : `OT${period - 2}`;
  return `P${period}`;
}

function formatTipoff(timeStr: string) {
  if (!timeStr) return 'TBD';
  try {
    return new Date(timeStr).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true, timeZoneName: 'short' });
  } catch { return timeStr; }
}

function TeamBlock({ name, score, state, isWinning }: { name: string; score: string; state: string; isWinning: boolean }) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, padding: '20px 24px' }}>
      <div style={{
        fontSize: 11, fontFamily: 'var(--font-mono)', letterSpacing: '0.1em',
        color: isWinning && state === 'post' ? 'var(--accent)' : 'var(--text-muted)',
        textTransform: 'uppercase', textAlign: 'center',
      }}>
        {name}
      </div>
      <div style={{
        fontSize: 52, fontWeight: 800, fontFamily: 'var(--font-display)', lineHeight: 1,
        color: state === 'pre' ? 'var(--text-muted)' : isWinning && state === 'post' ? 'var(--text-primary)' : state === 'in' ? 'var(--text-primary)' : 'var(--text-secondary)',
        textShadow: isWinning && state === 'post' ? '0 0 40px rgba(0,229,196,0.2)' : 'none',
      }}>
        {state === 'pre' ? '—' : score}
      </div>
      {isWinning && state === 'post' && (
        <div style={{ fontSize: 9, fontFamily: 'var(--font-mono)', color: 'var(--accent)', letterSpacing: '0.15em' }}>WIN</div>
      )}
    </div>
  );
}

export default function ScoreBoard({ score, sportKey, homeTeam, awayTeam, commenceTime }: { score?: ScoreData; sportKey: string; homeTeam?: string; awayTeam?: string; commenceTime?: string }) {
  if (!score) {
    const fallback: ScoreData = {
      home_team: homeTeam ?? '',
      away_team: awayTeam ?? '',
      home_score: '0',
      away_score: '0',
      status: 'pre',
      state: 'pre',
      period: 0,
      display_clock: '',
      commence_time: commenceTime ?? '',
    };
    return <ScoreBoard score={fallback} sportKey={sportKey} homeTeam={homeTeam} awayTeam={awayTeam} commenceTime={commenceTime} />;
  }

  const { home_team, away_team, home_score, away_score, state, period, display_clock, commence_time } = score;

  const homeScoreNum = parseInt(home_score || '0');
  const awayScoreNum = parseInt(away_score || '0');
  const homeWinning = homeScoreNum >= awayScoreNum;

  let statusBar: React.ReactNode;
  if (state === 'in') {
    const p = periodLabel(sportKey, period);
    statusBar = (
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <div style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--red)', boxShadow: '0 0 8px var(--red)', animation: 'pulse 1s infinite', flexShrink: 0 }} />
        <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--red)', fontSize: 12, fontWeight: 700, letterSpacing: '0.1em' }}>
          LIVE{p ? ` · ${p}` : ''}{display_clock ? ` · ${display_clock}` : ''}
        </span>
      </div>
    );
  } else if (state === 'post') {
    statusBar = (
      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontSize: 11, letterSpacing: '0.15em' }}>FINAL</span>
    );
  } else {
    statusBar = (
      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontSize: 11, letterSpacing: '0.1em' }}>
        TIP-OFF {formatTipoff(commence_time)}
      </span>
    );
  }

  return (
    <div style={{
      background: 'linear-gradient(135deg, #0b1420 0%, #0d1828 100%)',
      border: `1px solid ${state === 'in' ? 'rgba(255,71,87,0.3)' : 'var(--border-bright)'}`,
      borderRadius: 14, marginBottom: 16, overflow: 'hidden',
      boxShadow: state === 'in' ? '0 0 30px rgba(255,71,87,0.08)' : '0 4px 20px rgba(0,0,0,0.3)',
    }}>
      {/* Status bar */}
      <div style={{
        borderBottom: '1px solid var(--border)',
        padding: '10px 20px',
        display: 'flex', justifyContent: 'center', alignItems: 'center',
        background: state === 'in' ? 'rgba(255,71,87,0.05)' : 'rgba(255,255,255,0.02)',
      }}>
        {statusBar}
      </div>

      {/* Scores */}
      <div style={{ display: 'flex', alignItems: 'stretch' }}>
        <TeamBlock name={away_team} score={away_score} state={state} isWinning={awayScoreNum > homeScoreNum} />

        {/* Divider */}
        <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', padding: '20px 0', gap: 4 }}>
          <div style={{ width: 1, height: 40, background: 'var(--border)' }} />
          <span style={{ color: 'var(--border-bright)', fontSize: 11, fontFamily: 'var(--font-mono)', padding: '4px 0' }}>VS</span>
          <div style={{ width: 1, height: 40, background: 'var(--border)' }} />
        </div>

        <TeamBlock name={home_team} score={home_score} state={state} isWinning={homeScoreNum > awayScoreNum} />
      </div>

      {/* Bottom accent for live games */}
      {state === 'in' && (
        <div style={{ height: 2, background: 'linear-gradient(90deg, transparent, var(--red), transparent)', animation: 'scanline 2s ease-in-out infinite' }} />
      )}

      <style>{`
        @keyframes scanline {
          0%, 100% { opacity: 0.3; }
          50% { opacity: 1; }
        }
      `}</style>
    </div>
  );
}
