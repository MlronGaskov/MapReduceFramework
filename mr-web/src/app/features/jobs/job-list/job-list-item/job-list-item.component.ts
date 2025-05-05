import { Input, Output, ChangeDetectionStrategy, Component, EventEmitter } from '@angular/core';
import { MatIconModule } from '@angular/material/icon';
import { DatePipe } from '@angular/common';

export interface JobSummary {
  id: number;
  name: string;
  createdAt: string;
}

@Component({
  selector: 'app-job-list-item',
  imports: [MatIconModule, DatePipe],
  templateUrl: './job-list-item.component.html',
  styleUrl: './job-list-item.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobListItemComponent {
  @Input() job!: JobSummary;

  @Output() open = new EventEmitter<number>();

  @Output() remove = new EventEmitter<number>();
}
