import { Input, Output, ChangeDetectionStrategy, Component, EventEmitter } from '@angular/core';
import { MatIconModule } from '@angular/material/icon';
import { DatePipe } from '@angular/common';

export interface JobSummary {
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
  @Input() job!: JobSummary;

  @Output() open = new EventEmitter<number>();

  @Output() remove = new EventEmitter<number>();
}
