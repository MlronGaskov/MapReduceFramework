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
  ngOnInit(): void {
    //this.reload();
  }

  jobs$!: Observable<JobSummary[]>;

  constructor(
    private readonly jobService: JobService,
    private readonly dialog: MatDialog
  ) {
    //this.jobs$ = this.jobService.getJobs();
  }

  openJob(jobId: number): void {
    this.dialog.open(JobDetailsDialogComponent, {
      width: '600px',
      data: { jobId },
    });
  }

  deleteJob(jobId: number): void {
    this.jobService.deleteJob(jobId).subscribe({
      next: () => console.log('Job deleted', jobId),
      error: err => console.error(err),
    });
  }

  reload(): void {
    this.jobs$ = this.jobService.getJobs();
  }
}
