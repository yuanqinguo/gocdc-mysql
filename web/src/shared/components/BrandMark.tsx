export function BrandMark({ className }: { className?: string }) {
  return (
    <span className={`brand-mark ${className ?? ''}`} aria-hidden="true">
      <svg viewBox="0 0 32 32" width="100%" height="100%" fill="none">
        <rect x="1.5" y="1.5" width="29" height="29" rx="4" stroke="currentColor" strokeWidth="1.4" />
        <path d="M8 23V16M13 23V9M18 23V18M23 23V13" stroke="currentColor" strokeWidth="2" />
        <circle cx="23" cy="8" r="2" fill="currentColor" />
      </svg>
    </span>
  )
}
