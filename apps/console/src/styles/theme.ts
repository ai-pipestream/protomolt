import type { ThemeDefinition } from 'vuetify'

/**
 * The molt palette. Dark-first: deep graphite ink with a copper-amber accent
 * (the shed shell), teal for informational states, and unambiguous
 * success/error hues. The light theme is the same system on paper.
 */
export const moltDark: ThemeDefinition = {
  dark: true,
  colors: {
    background: '#101418',
    surface: '#171c22',
    'surface-bright': '#1e242c',
    'surface-variant': '#232a33',
    'on-surface-variant': '#aeb7c2',
    primary: '#e8a33d',
    'primary-darken-1': '#c9882a',
    secondary: '#4fd1c5',
    accent: '#e8a33d',
    error: '#f26d6d',
    info: '#6cb6ff',
    success: '#57c78a',
    warning: '#e8c53d',
  },
  variables: {
    'border-color': '#8a93a0',
    'border-opacity': 0.18,
    'medium-emphasis-opacity': 0.72,
  },
}

export const moltLight: ThemeDefinition = {
  dark: false,
  colors: {
    background: '#fbfbf9',
    surface: '#ffffff',
    'surface-bright': '#ffffff',
    'surface-variant': '#eef0f3',
    'on-surface-variant': '#4a4e57',
    primary: '#b4761a',
    'primary-darken-1': '#96610f',
    secondary: '#0e8a7d',
    accent: '#b4761a',
    error: '#c62f2f',
    info: '#2f5d78',
    success: '#2e7d4f',
    warning: '#9a6a3a',
  },
  variables: {
    'border-color': '#1c1e23',
    'border-opacity': 0.14,
    'medium-emphasis-opacity': 0.68,
  },
}
