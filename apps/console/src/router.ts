import { createRouter, createWebHistory } from 'vue-router'

// Subjects may contain slashes ("example/person.proto"); links are built
// with encodeURIComponent and the catch-all decodes on match.
export const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', redirect: '/schema-registry' },
    {
      path: '/schema-registry',
      component: () => import('./App.vue'),
      children: [
        {
          path: '',
          name: 'schema-registry-subjects',
          component: () => import('./views/SubjectsView.vue'),
        },
        {
          path: 'chains',
          name: 'schema-registry-chains',
          component: () => import('./views/ChainsView.vue'),
        },
        {
          path: 'merge',
          name: 'schema-registry-merge',
          component: () => import('./views/MergeWorkbenchView.vue'),
        },
        {
          path: 'connect',
          name: 'schema-registry-connect',
          component: () => import('./views/ConnectServiceView.vue'),
        },
        {
          path: 'subjects/:subject(.*)',
          name: 'schema-registry-subject',
          component: () => import('./views/SubjectDetailView.vue'),
        },
      ],
    },
  ],
})
