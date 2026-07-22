import '@fontsource-variable/inter'
import '@fontsource-variable/jetbrains-mono'
import '@mdi/font/css/materialdesignicons.css'
import 'vuetify/styles'
import './styles/console.css'

import { createApp } from 'vue'
import { createVuetify } from 'vuetify'
import * as components from 'vuetify/components'
import * as directives from 'vuetify/directives'

import Root from './Root.vue'
import { router } from './router'
import { moltDark, moltLight } from './styles/theme'

const vuetify = createVuetify({
  components,
  directives,
  theme: {
    defaultTheme: localStorage.getItem('protomolt-theme') === 'light' ? 'moltLight' : 'moltDark',
    themes: { moltDark, moltLight },
  },
  defaults: {
    VCard: { flat: true, border: true, rounded: 'lg' },
    VBtn: { rounded: 'lg' },
    VTextField: { variant: 'outlined', density: 'comfortable' },
    VTextarea: { variant: 'outlined', density: 'comfortable' },
    VSelect: { variant: 'outlined', density: 'comfortable' },
    VChip: { rounded: 'lg' },
  },
})

createApp(Root).use(vuetify).use(router).mount('#app')
