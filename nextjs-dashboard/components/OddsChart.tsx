'use client';
import { useState, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ReferenceLine, ResponsiveContainer,
} from 'recharts';

const COLORS = ['#00e5c4', '#4dabf7', '#ff6b9d', '#ffa502', '#a29bfe', '#55efc4', '#fd79a8', '#74b9ff'];
const RANGES = [
  { label: '1H', hours: 1 },
  { label: '3H', hours: 3 },
  { label: '6H', hours: 6 },
  { label: 'All', hours: null },
];

interface OddsRow {
  team: string;
  bookmaker_key: string;
  computed_at: string;
  american_odds: number;
}

function fmtOdds(v: number) {
  const n = Math.round(Number(v));
  return n > 0 ? `+${n}` : `${n}`;
}

function fmtTime(ts: string) {
  return new Date(ts).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true });
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: '#0d1520', border: '1px solid #243548', borderRadius: 8, padding: '10px 14px', fontSize: 12, fontFamily: 'Space Mono, monospace' }}>
      <div style={{ color: '#6e8caa', marginBottom: 6 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.dataKey} style={{ color: p.color, marginBottom: 3 }}>
          {p.name}: <span style={{ color: '#eaf2ff', fontWeight: 700 }}>{fmtOdds(p.value)}</span>
        </div>
      ))}
    </div>
  );
};

export default function OddsChart({ data, title }: { data: OddsRow[]; title: string }) {
  const [range, setRange] = useState<number | null>(null);
  const [hidden, setHidden] = useState<Set<string>>(new Set());

  const { seriesKeys, colorMap, teamGroups } = useMemo(() => {
    const valid = data.filter(r => {
      const v = Number(r.american_odds);
      return !isNaN(v) && v >= -2500 && v <= 2500;
    });
    const seen = new Set<string>();
    const keys: string[] = [];
    for (const r of valid) {
      const key = `${r.team}__${r.bookmaker_key}`;
      if (!seen.has(key)) { seen.add(key); keys.push(key); }
    }
    const cmap: Record<string, string> = {};
    keys.forEach((k, i) => { cmap[k] = COLORS[i % COLORS.length]; });
    const groups: Record<string, string[]> = {};
    for (const key of keys) {
      const team = key.split('__')[0];
      if (!groups[team]) groups[team] = [];
      groups[team].push(key);
    }
    return { seriesKeys: keys, colorMap: cmap, teamGroups: groups };
  }, [data]);

  const clean = useMemo(() => {
    const valid = data.filter(r => {
      const v = Number(r.american_odds);
      return !isNaN(v) && v >= -2500 && v <= 2500;
    });
    if (!range) return valid;
    const cutoff = Date.now() - range * 60 * 60 * 1000;
    return valid.filter(r => new Date(r.computed_at).getTime() >= cutoff);
  }, [data, range]);

  const chartData = useMemo(() => {
    const byTime: Record<string, any> = {};
    for (const r of clean) {
      const t = fmtTime(r.computed_at);
      const ts = new Date(r.computed_at).getTime();
      if (!byTime[t]) byTime[t] = { time: t, _ts: ts };
      byTime[t][`${r.team}__${r.bookmaker_key}`] = Number(r.american_odds);
    }
    return Object.values(byTime).sort((a: any, b: any) => a._ts - b._ts);
  }, [clean]);

  const toggleLine = (key: string) => {
    setHidden(prev => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  };

  const toggleTeam = (team: string) => {
    const keys = teamGroups[team] || [];
    const allHidden = keys.every(k => hidden.has(k));
    setHidden(prev => {
      const next = new Set(prev);
      if (allHidden) keys.forEach(k => next.delete(k));
      else keys.forEach(k => next.add(k));
      return next;
    });
  };

  return (
    <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8, padding: 16, marginBottom: 16 }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', letterSpacing: '0.1em', fontFamily: 'var(--font-mono)' }}>
          {title.toUpperCase()}
        </div>
        <div style={{ display: 'flex', gap: 4 }}>
          {RANGES.map(r => (
            <button key={r.label} onClick={() => setRange(r.hours)} style={{
              background: range === r.hours ? 'var(--accent)' : 'var(--bg-secondary)',
              color: range === r.hours ? '#040d14' : 'var(--text-muted)',
              border: '1px solid var(--border-bright)', borderRadius: 5,
              padding: '3px 10px', fontSize: 11, cursor: 'pointer',
              fontFamily: 'var(--font-mono)', fontWeight: range === r.hours ? 700 : 400,
              transition: 'all 0.15s',
            }}>{r.label}</button>
          ))}
        </div>
      </div>

      {/* Legend grouped by team */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10, marginBottom: 16 }}>
        {Object.entries(teamGroups).map(([team, keys]) => {
          const allHidden = keys.every(k => hidden.has(k));
          return (
            <div key={team}>
              <button onClick={() => toggleTeam(team)} style={{
                background: 'none', border: 'none', cursor: 'pointer',
                display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, width: '100%',
                opacity: allHidden ? 0.4 : 1, transition: 'opacity 0.15s',
              }}>
                <div style={{ width: 16, height: 1, background: 'var(--border-bright)' }} />
                <span style={{ fontSize: 10, color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)', letterSpacing: '0.1em', textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{team}</span>
                <div style={{ flex: 1, height: 1, background: 'var(--border)' }} />
                <span style={{ fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{allHidden ? 'show' : 'hide'}</span>
              </button>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, paddingLeft: 8 }}>
                {keys.map(key => {
                  const book = key.split('__')[1];
                  const color = colorMap[key];
                  const isHidden = hidden.has(key);
                  return (
                    <button key={key} onClick={() => toggleLine(key)} style={{
                      display: 'flex', alignItems: 'center', gap: 5,
                      background: isHidden ? 'transparent' : `${color}18`,
                      border: `1px solid ${isHidden ? 'var(--border)' : color}`,
                      borderRadius: 20, padding: '3px 10px', cursor: 'pointer',
                      opacity: isHidden ? 0.35 : 1, transition: 'all 0.15s',
                    }}>
                      <div style={{ width: 14, height: 2, background: color, borderRadius: 1 }} />
                      <span style={{ fontSize: 10, color: isHidden ? 'var(--text-muted)' : color, fontFamily: 'var(--font-mono)' }}>{book}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>

      {/* Chart area — always rendered, empty state shown inside */}
      {chartData.length === 0 ? (
        <div style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center', border: '1px dashed var(--border)', borderRadius: 8 }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ color: 'var(--text-muted)', fontSize: 12, fontFamily: 'var(--font-mono)', marginBottom: 8 }}>No data for this time range</div>
            <button onClick={() => setRange(null)} style={{
              background: 'var(--accent)', color: '#040d14', border: 'none',
              borderRadius: 5, padding: '4px 12px', fontSize: 11, cursor: 'pointer',
              fontFamily: 'var(--font-mono)', fontWeight: 700,
            }}>Show All</button>
          </div>
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData} margin={{ top: 8, right: 16, bottom: 8, left: 16 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1a2535" vertical={false} />
            <XAxis dataKey="time" tick={{ fill: '#4a6580', fontSize: 10, fontFamily: 'Space Mono, monospace' }} tickLine={false} axisLine={{ stroke: '#1a2535' }} interval="preserveStartEnd" />
            <YAxis tickFormatter={fmtOdds} tick={{ fill: '#4a6580', fontSize: 10, fontFamily: 'Space Mono, monospace' }} tickLine={false} axisLine={false} width={48} />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine y={0} stroke="#243548" strokeDasharray="4 4" />
            {seriesKeys.map(key => (
              !hidden.has(key) && (
                <Line key={key} type="monotone" dataKey={key}
                  name={key.split('__')[1]}
                  stroke={colorMap[key]} strokeWidth={2}
                  dot={{ r: 3, fill: colorMap[key], strokeWidth: 0 }}
                  activeDot={{ r: 6, strokeWidth: 0 }}
                  connectNulls isAnimationActive={false}
                />
              )
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
