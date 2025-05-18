import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Observable } from 'rxjs';
import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { JobService, JobInfo, JobProgress } from '../job.service';

import { MatIconModule } from '@angular/material/icon';
import { MatDialogModule } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';

interface DialogData { jobId: number; }

@Component({
  selector: 'app-job-details-dialog',
  imports: [AsyncPipe, MatDialogModule, MatDividerModule, MatIconModule, NgIf, NgFor],
  templateUrl: './job-details-dialog.component.html',
  styleUrl: './job-details-dialog.component.scss'
})
export class JobDetailsDialogComponent {
  jobInfo$!: Observable<JobInfo>;
  progress$!: Observable<JobProgress>;

  constructor(
    @Inject(MAT_DIALOG_DATA) readonly data: DialogData,
    private readonly jobService: JobService
  ) {
    this.refreshProgress();
  }

  private loadStatic(): void {
    this.jobInfo$ = this.jobService.getJobInfo(this.data.jobId - 1);
    console.log(this.jobInfo$);
  }

  refreshProgress(): void {
    this.loadStatic();
    this.progress$ = this.jobService.getProgress(this.data.jobId - 1);
  }
}
