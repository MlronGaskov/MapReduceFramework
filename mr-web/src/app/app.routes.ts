import { Routes } from '@angular/router';
import { JobDashboardComponent } from './features/jobs/job-dashboard/job-dashboard.component';

export const routes: Routes = [
  {
      path: '',
      pathMatch: 'full',
      loadComponent() {
          return import('./features/jobs/job-dashboard/job-dashboard.component').then((m) => m.JobDashboardComponent)
      }
  },
  { path: '**', redirectTo: '' }
];
