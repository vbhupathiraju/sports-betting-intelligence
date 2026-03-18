'use client';
import { useState, useEffect } from 'react';

export default function Clock() {
  const [time, setTime] = useState('');

  useEffect(() => {
    const update = () => setTime(new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true }));
    update();
    const t = setInterval(update, 1000);
    return () => clearInterval(t);
  }, []);

  if (!time) return null;

  return (
    <span style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: 'DM Mono, monospace' }}>
      {time}
    </span>
  );
}
