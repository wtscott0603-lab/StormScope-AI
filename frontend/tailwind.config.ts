/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Panel chrome
        surface: '#1a1d21',
        panel: '#22262c',
        panelBorder: '#333840',
        panelHover: '#2a2e36',
        // Text
        textPrimary: '#e4e6ea',
        textSecondary: '#8a9099',
        textMuted: '#5c636e',
        // Accent — muted blue like Radar Omega
        accent: '#5b9bd5',
        accentHover: '#4a8bc4',
        // Status
        danger: '#d94f4f',
        warning: '#d98c2a',
        ok: '#4caf76',
        // SPC categorical colors (NWS standard)
        spcTstm: '#c1e9c1',
        spcMrgl: '#66a366',
        spcSlgt: '#f6f67b',
        spcEnh: '#e6b366',
        spcMdt: '#e66666',
        spcHigh: '#ff66ff',
      },
      boxShadow: {
        panel: '0 4px 16px rgba(0,0,0,0.5)',
        dropdown: '0 6px 20px rgba(0,0,0,0.6)',
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'monospace'],
      },
      keyframes: {
        blink: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.3' },
        },
      },
      animation: {
        blink: 'blink 1.2s ease-in-out infinite',
      },
    },
  },
  plugins: [],
}
