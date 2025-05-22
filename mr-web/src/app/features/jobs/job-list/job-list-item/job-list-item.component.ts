import { Input, Output, ChangeDetectionStrategy, Component, EventEmitter } from '@angular/core';
import { MatIconModule } from '@angular/material/icon';

import { JobService } from '../../job.service';
import { MatSnackBar } from '@angular/material/snack-bar';

export interface JobSummary {
  backendIndex: number;
  jobId: number;
  jobName: string;
  submissionTime: string;
}

@Component({
  selector: 'app-job-list-item',
  imports: [MatIconModule],
  templateUrl: './job-list-item.component.html',
  styleUrl: './job-list-item.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobListItemComponent {
    constructor(
    private readonly jobService: JobService,
    private readonly snack: MatSnackBar
  ) {}

  @Input() job!: JobSummary;

  @Output() open = new EventEmitter<number>();

  @Output() deleted = new EventEmitter<void>();

  delete(): void {
  this.jobService.deleteJob(this.job.backendIndex).subscribe({
    next: () => this.deleted.emit(),
    error: err => this.snack.open('Delete failed', 'Close', {duration: 3000}),
  });
}

}
