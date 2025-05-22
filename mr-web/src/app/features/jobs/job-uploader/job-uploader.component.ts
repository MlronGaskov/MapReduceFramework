import { Component, EventEmitter, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { finalize } from 'rxjs/operators';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatCardModule } from '@angular/material/card';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';

import { JobService, UploadJobRequest } from '../job.service';
import { NgIf } from '@angular/common';

@Component({
  selector: 'app-job-uploader',
  imports: [MatCardModule, MatFormFieldModule, MatInputModule, ReactiveFormsModule, NgIf],
  templateUrl: './job-uploader.component.html',
  styleUrl: './job-uploader.component.scss'
})
export class JobUploaderComponent {
  @Output() jobUploaded = new EventEmitter<void>();

  form!: FormGroup;
  isSubmitting = false;

  constructor(
    private readonly fb: FormBuilder,
    private readonly jobService: JobService,
    private readonly snack: MatSnackBar
  ) {
      this.form = this.fb.group({
        jobId: [1,  Validators.required],
        jobName: ['', Validators.required],

        jobPath: ['', Validators.required],
        jobStorageConnectionString: ['', Validators.required],

        dataStorageConnectionString: ['', Validators.required],
        inputsPath: ['', Validators.required],
        mappersOutputsPath: ['', Validators.required],
        reducersOutputsPath: ['', Validators.required],

        mappersCount: [1,  [Validators.required, Validators.min(1)]],
        reducersCount: [1,  [Validators.required, Validators.min(1)]],
        sorterInMemoryRecords: [10000,  [Validators.required, Validators.min(10000)]],
      });
  }

  upload(): void {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    this.jobService.setCoordinatorUrl();

    const req: UploadJobRequest = {
      jobId: this.form.value.jobId,
      jobName: this.form.value.jobName,

      jobPath: this.form.value.jobPath,
      jobStorageConnectionString: this.form.value.jobStorageConnectionString,
      dataStorageConnectionString: this.form.value.dataStorageConnectionString,

      inputsPath: this.form.value.inputsPath,
      mappersOutputsPath:  this.form.value.mappersOutputsPath,
      reducersOutputsPath: this.form.value.reducersOutputsPath,

      mappersCount: this.form.value.mappersCount,
      reducersCount: this.form.value.reducersCount,
      sorterInMemoryRecords: this.form.value.sorterInMemoryRecords,
    };

    this.isSubmitting = true;
    this.jobService
      .uploadJob(req)
      .pipe(finalize(() => (this.isSubmitting = false)))
      .subscribe({
        next: text => {
          this.snack.open(text, 'OK', { duration: 2500 });
          this.form.reset();
          this.jobUploaded.emit();
        },
        error: err => {
          console.error(err);
          this.snack.open('Upload failed', 'Close', { duration: 3000 });
        },
      });
  }
}
