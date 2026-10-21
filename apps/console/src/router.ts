import { createRouter, createWebHistory } from 'vue-router'

// Subjects may contain slashes ("example/person.proto"); links are built
// with encodeURIComponent and the catch-all decodes on match.
export const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/', redirect: '/tasks' },
    // The repository console's sign-in page; a failed login lands here with its reason.
    { path: '/sign-in', name: 'sign-in', component: () => import('./views/SignInView.vue') },
    { path: '/start', name: 'starter', component: () => import('./views/CorrectionStarterView.vue') },
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
    {
      path: '/repository',
      component: () => import('./views/repository/RepositoryShell.vue'),
      children: [
        {
          path: '',
          name: 'repository',
          component: () => import('./views/repository/RepositoryAccountsView.vue'),
        },
        {
          path: 'accounts/:accountId',
          name: 'repository-account',
          component: () => import('./views/repository/RepositoryAccountView.vue'),
        },
        {
          // The call number rides in the query (account, graph, node, doc, and
          // an optional exact revision): its parts may hold slashes and colons.
          path: 'document',
          name: 'repository-document',
          component: () => import('./views/repository/RepositoryDocumentView.vue'),
        },
        {
          path: 'archive',
          name: 'repository-archive',
          component: () => import('./views/repository/RepositoryArchiveView.vue'),
        },
        {
          path: 'archive-entry',
          name: 'repository-archive-entry',
          component: () => import('./views/repository/RepositoryArchiveEntryView.vue'),
        },
        {
          path: 'lookup',
          name: 'repository-lookup',
          component: () => import('./views/repository/RepositoryLookupView.vue'),
        },
      ],
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
