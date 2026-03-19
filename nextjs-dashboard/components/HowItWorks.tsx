'use client';
import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

export default function HowItWorks() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    try {
      const seen = localStorage.getItem('hasSeenOnboarding');
      if (!seen) setOpen(true);
    } catch {}
  }, []);

  const handleClose = () => {
    setOpen(false);
    try { localStorage.setItem('hasSeenOnboarding', 'true'); } catch {}
  };

  return (
    <>
      {/* "How does this work?" button */}
      <button
        onClick={() => setOpen(true)}
        style={{
          background: 'transparent',
          border: '1px solid var(--border-bright)',
          borderRadius: 8,
          padding: '5px 12px',
          color: 'var(--text-muted)',
          fontSize: 11,
          fontFamily: 'var(--font-mono)',
          letterSpacing: '0.08em',
          cursor: 'pointer',
          transition: 'all 0.18s',
          whiteSpace: 'nowrap',
        }}
        onMouseEnter={e => {
          (e.target as HTMLButtonElement).style.color = 'var(--accent)';
          (e.target as HTMLButtonElement).style.borderColor = 'var(--accent)';
        }}
        onMouseLeave={e => {
          (e.target as HTMLButtonElement).style.color = 'var(--text-muted)';
          (e.target as HTMLButtonElement).style.borderColor = 'var(--border-bright)';
        }}
      >
        ? How does this work
      </button>

      {/* Modal */}
      <AnimatePresence>
        {open && (
          <>
            {/* Backdrop */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={handleClose}
              style={{
                position: 'fixed', inset: 0,
                background: 'rgba(4, 13, 20, 0.85)',
                backdropFilter: 'blur(4px)',
                zIndex: 1000,
              }}
            />

            {/* Modal panel */}
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              transition={{ duration: 0.22, ease: [0.16, 1, 0.3, 1] }}
              style={{
                position: 'fixed',
                top: 0, left: 0, right: 0, bottom: 0,
                margin: 'auto',
                zIndex: 1001,
                width: 'calc(100vw - 40px)', maxWidth: 860,
                alignSelf: 'center',
                maxHeight: '90vh',
                overflowY: 'auto',
                background: 'var(--bg-card)',
                border: '1px solid var(--border-bright)',
                borderRadius: 16,
                padding: '32px 32px 24px',
                boxShadow: '0 24px 80px rgba(0,0,0,0.6)',
              }}
            >
              {/* Header */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 }}>
                <div>
                  <div style={{ fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 800, color: 'var(--text-primary)', marginBottom: 6 }}>
                    How does <span style={{ color: 'var(--accent)' }}>SharpEdge</span> work?
                  </div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)', letterSpacing: '0.1em' }}>
                    TWO SIGNALS. ONE EDGE.
                  </div>
                </div>
                <button
                  onClick={handleClose}
                  style={{
                    background: 'transparent', border: '1px solid var(--border)',
                    borderRadius: 8, width: 32, height: 32,
                    color: 'var(--text-muted)', fontSize: 16, cursor: 'pointer',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexShrink: 0,
                  }}
                >✕</button>
              </div>

              {/* Cards */}
              <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>

                {/* Divergence */}
                <div style={{
                  flex: 1, minWidth: 260,
                  background: 'var(--bg-secondary)',
                  border: '1px solid rgba(0,229,196,0.2)',
                  borderRadius: 12, padding: '20px 22px',
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
                    <span style={{ fontSize: 18 }}>⟆</span>
                    <span style={{ fontFamily: 'var(--font-display)', fontSize: 15, fontWeight: 700, color: 'var(--accent)' }}>
                      Divergence Signals
                    </span>
                  </div>
                  <p style={{ fontFamily: 'var(--font-display)', fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7, margin: '0 0 16px' }}>
                    Kalshi is a federally regulated prediction market where traders bet real money on game outcomes. Unlike sportsbooks, Kalshi prices are driven purely by crowd wisdom — no house edge, no line shading. (by definition)
                  </p>
                  <p style={{ fontFamily: 'var(--font-display)', fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7, margin: '0 0 16px' }}>
                    A divergence signal appears when Kalshi's implied win probability for a team differs significantly from the consensus across major sportsbooks (FanDuel, DraftKings, BetMGM, BetRivers). When these two markets disagree by 5% or more, it suggests one side has information or conviction the other doesn't.
                  </p>
                  <div style={{
                    background: 'rgba(0,229,196,0.06)', border: '1px solid rgba(0,229,196,0.15)',
                    borderRadius: 8, padding: '12px 14px',
                  }}>
                    <div style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--accent)', letterSpacing: '0.12em', marginBottom: 6 }}>HOW TO USE IT</div>
                    <p style={{ fontFamily: 'var(--font-display)', fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6, margin: 0 }}>
                      A large divergence — especially if Kalshi is higher on a team — can indicate smart money flowing into the prediction market ahead of sportsbooks adjusting their lines. Consider it a flag to dig deeper on that game.
                    </p>
                  </div>
                </div>

                {/* Sharp Money */}
                <div style={{
                  flex: 1, minWidth: 260,
                  background: 'var(--bg-secondary)',
                  border: '1px solid rgba(100,160,255,0.2)',
                  borderRadius: 12, padding: '20px 22px',
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
                    <span style={{ fontSize: 18 }}>◈</span>
                    <span style={{ fontFamily: 'var(--font-display)', fontSize: 15, fontWeight: 700, color: 'var(--blue)' }}>
                      Sharp Money Signals
                    </span>
                  </div>
                  <p style={{ fontFamily: 'var(--font-display)', fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7, margin: '0 0 16px' }}>
                    Sharp bettors are professional or high-volume bettors whose action consistently moves lines. When a sportsbook line moves significantly in a short period without an obvious public reason, it often indicates sharp money coming in on one side.
                  </p>
                  <p style={{ fontFamily: 'var(--font-display)', fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7, margin: '0 0 16px' }}>
                    A sharp money signal appears when a team's moneyline odds shift by 10 or more points between polling cycles across major books. The direction of the move tells you which side the sharp action is on — odds shortening means money is coming in on that team.
                  </p>
                  <div style={{
                    background: 'rgba(100,160,255,0.06)', border: '1px solid rgba(100,160,255,0.15)',
                    borderRadius: 8, padding: '12px 14px',
                  }}>
                    <div style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--blue)', letterSpacing: '0.12em', marginBottom: 6 }}>HOW TO USE IT</div>
                    <p style={{ fontFamily: 'var(--font-display)', fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6, margin: 0 }}>
                      Line movement without public cause is one of the most reliable indicators of informed betting activity. Use these signals to identify which side the sharps are on before the line moves further.
                    </p>
                  </div>
                </div>
              </div>

              {/* Footer */}
              <div style={{ marginTop: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12 }}>
                <span style={{ fontFamily: 'var(--font-display)', fontSize: 12, color: 'var(--text-muted)', fontStyle: 'italic', opacity: 0.8 }}>
                  *not financial advice
                </span>
                <button
                  onClick={handleClose}
                  style={{
                    background: 'var(--accent)', color: '#040d14',
                    border: 'none', borderRadius: 8,
                    padding: '10px 28px',
                    fontSize: 13, fontWeight: 700,
                    fontFamily: 'var(--font-display)',
                    cursor: 'pointer',
                    letterSpacing: '0.04em',
                  }}
                >
                  Got it
                </button>
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </>
  );
}
