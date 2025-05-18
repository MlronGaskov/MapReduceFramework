import { Component } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { AsyncPipe, NgFor } from '@angular/common';
import { Observable } from 'rxjs';

import { JobListItemComponent } from './job-list-item/job-list-item.component';
import { JobService } from '../job.service';
import { JobDetailsDialogComponent } from '../job-details-dialog/job-details-dialog.component';
import { JobSummary } from './job-list-item/job-list-item.component';
import { MatSnackBar } from '@angular/material/snack-bar';

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
    private readonly snack: MatSnackBar
  ) {}

  openJob(jobId: number): void {
    this.dialog.open(JobDetailsDialogComponent, {
      width: '600px',
      data: { jobId },
    });
  }

  // deleteJob(jobId: number): void {
  //   this.jobService.deleteJob(jobId - 1).subscribe({
  //     next: () => {
  //       this.snack.open(`Job ${jobId} deleted`, 'Close', { duration: 3000 })
  //       this.reload();
  //     },  
  //     error: err => this.snack.open('Delete failed', 'Close', { duration: 3000 }),
  //   });
  // }

  reload(): void {
    console.log("Reload.");
    this.jobs$ = this.jobService.getJobs()
    this.jobs$.forEach((j) => console.log(j));
  }
}
