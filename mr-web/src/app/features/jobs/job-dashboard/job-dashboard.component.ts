import { Component } from '@angular/core';
import { JobUploaderComponent } from '../job-uploader/job-uploader.component';
import { JobListComponent } from '../job-list/job-list.component';

@Component({
  selector: 'app-job-dashboard',
  imports: [JobUploaderComponent, JobListComponent],
  templateUrl: './job-dashboard.component.html',
  styleUrl: './job-dashboard.component.scss'
})
export class JobDashboardComponent {
  /** метод придёт от JobUploader → пробрасываем вниз в JobList (через template reference) */
  refreshList(list: any) {
    list?.reload();
  }
}
