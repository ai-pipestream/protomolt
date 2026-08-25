import { createRouter, createWebHistory } from 'vue-router'

// Subjects may contain slashes ("example/person.proto"); links are built
// with encodeURIComponent and the catch-all decodes on match.
export const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', redirect: '/tasks' },
    {
      path: '/tasks',
      name: 'tasks',
      component: () => import('./views/TaskConsoleView.vue'),
    },
    {
      path: '/workflows',
      name: 'workflows',
      component: () => import('./views/WorkflowsView.vue'),
    },
    {
      path: '/services',
      name: 'services',
      component: () => import('./views/ServicesView.vue'),
    },
    {
      path: '/services/connect',
      name: 'services-connect',
      component: () => import('./views/ConnectServiceView.vue'),
    },
    {
      path: '/search',
      name: 'search',
      component: () => import('./views/SearchView.vue'),
    },
    {
      path: '/metrics',
      name: 'metrics',
      component: () => import('./views/MetricsView.vue'),
    },
    {
      path: '/receipts',
      name: 'receipts',
      component: () => import('./views/ReceiptsView.vue'),
    },
    // Pre-section paths that may live in bookmarks and shared links.
    { path: '/schema-registry/workflows', redirect: '/workflows' },
    { path: '/schema-registry/connect', redirect: '/services/connect' },
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
          path: 'merge',
          name: 'schema-registry-merge',
          component: () => import('./views/MergeWorkbenchView.vue'),
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
