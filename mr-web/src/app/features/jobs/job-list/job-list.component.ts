import { Component } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { AsyncPipe, NgFor } from '@angular/common';
import { Observable } from 'rxjs';

import { JobListItemComponent } from './job-list-item/job-list-item.component';
import { JobService } from '../job.service';
import { JobDetailsDialogComponent } from '../job-details-dialog/job-details-dialog.component';
import { JobSummary } from './job-list-item/job-list-item.component';

@Component({
  selector: 'app-job-list',
  imports: [JobListItemComponent, AsyncPipe, NgFor],
  templateUrl: './job-list.component.html',
  styleUrl: './job-list.component.scss'
})
export class JobListComponent {
  jobs$!: Observable<JobSummary[]>;

  constructor(
    private readonly jobService: JobService,
    private readonly dialog: MatDialog,
  ) {}

  openJob(idx: number): void {
    this.dialog.open(JobDetailsDialogComponent, {
      width: '600px',
      data: { idx },
    });
  }

  reload(): void {
    this.jobs$ = this.jobService.getJobs()
  }
}
