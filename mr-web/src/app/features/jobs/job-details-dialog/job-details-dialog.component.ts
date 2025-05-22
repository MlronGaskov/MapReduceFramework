import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Observable } from 'rxjs';
import { AsyncPipe, NgFor, NgIf } from '@angular/common';
import { JobService, JobInfo, JobProgress } from '../job.service';

import { MatIconModule } from '@angular/material/icon';
import { MatDialogModule } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';

interface DialogData { idx: number; }

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
    this.loadStatic();
    this.refreshProgress();
  }

  private loadStatic(): void {
    this.jobInfo$ = this.jobService.getJobInfo(this.data.idx);
  }

  refreshProgress(): void {
    this.progress$ = this.jobService.getProgress(this.data.idx);
  }

  storageType(conn: string): string {
    return conn.split(':')[0] == "S3" ? "S3" : "LOCAL";
  }
}
