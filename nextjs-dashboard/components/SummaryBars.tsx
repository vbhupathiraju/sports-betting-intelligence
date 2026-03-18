'use client';
import { motion } from 'framer-motion';

interface SummaryItem {
  label: string;
  value: number;
  color: string;
}

export default function SummaryBars({ items, title }: { items: SummaryItem[]; title: string }) {
  const maxVal = Math.max(...items.map(d => d.value), 1);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        borderRadius: 10,
        padding: '20px 28px',
        display: 'table',
      }}
    >
      <div style={{ fontSize: 10, color: 'var(--text-muted)', letterSpacing: '0.12em', marginBottom: 16, fontFamily: 'var(--font-mono)' }}>
        {title}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        {items.map((d, i) => (
          <motion.div
            key={d.label}
            initial={{ opacity: 0, x: -8 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: i * 0.04 }}
            style={{ display: 'flex', alignItems: 'center', gap: 16, whiteSpace: 'nowrap' }}
          >
            <div style={{
              fontSize: 12, color: 'var(--text-secondary)',
              width: 320, flexShrink: 0,
              fontFamily: 'var(--font-display)', fontWeight: 400,
            }}>
              {d.label}
            </div>
            <div style={{ width: 300, height: 5, background: 'var(--bg-secondary)', borderRadius: 3, flexShrink: 0, overflow: 'hidden' }}>
              <motion.div
                initial={{ width: '0%' }}
                animate={{ width: `${(d.value / maxVal) * 100}%` }}
                transition={{ duration: 0.8, delay: 0.1 + i * 0.05, ease: [0.16, 1, 0.3, 1] }}
                style={{
                  height: '100%',
                  background: `linear-gradient(90deg, ${d.color}, ${d.color}77)`,
                  borderRadius: 3,
                }}
              />
            </div>
            <div style={{
              fontSize: 13, color: d.color, fontWeight: 700,
              width: 52, textAlign: 'right', flexShrink: 0,
              fontFamily: 'var(--font-mono)',
            }}>
              {d.value.toFixed(1)}%
            </div>
          </motion.div>
        ))}
      </div>
    </motion.div>
  );
}
