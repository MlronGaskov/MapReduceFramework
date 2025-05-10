import { Component, EventEmitter, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { finalize } from 'rxjs/operators';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';

import { JobService, UploadJobRequest } from '../job.service';

@Component({
  selector: 'app-job-uploader',
  imports: [MatCardModule, MatFormFieldModule, ReactiveFormsModule],
  templateUrl: './job-uploader.component.html',
  styleUrl: './job-uploader.component.scss'
})
export class JobUploaderComponent {
  /** сообщаем родителю, что список нужно обновить */
  @Output() jobUploaded = new EventEmitter<void>();

  form!: FormGroup;
  isSubmitting = false;

  constructor(
    private readonly fb: FormBuilder,
    private readonly jobService: JobService,
    private readonly snack: MatSnackBar
  ) {
      this.form = this.fb.group({
        jobUrl : ['', [Validators.required]],
        jobName: ['', [Validators.required, Validators.maxLength(40)]],
      });
  }


  upload(): void {
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      return;
    }

    const req: UploadJobRequest = {
      jobName:  this.form.value.jobName!,
      userJar:  this.form.value.jobUrl!,
      mainClass: '',          // можно дать пользователю отдельное поле, упрощаем
      mappers:   1,
      reducers:  1,
      inputsPath:  '',
      outputsPath: '',
    };

    this.isSubmitting = true;
    this.jobService
      .uploadJob(req)
      .pipe(finalize(() => (this.isSubmitting = false)))
      .subscribe({
        next: res => {
          this.snack.open(`Job #${res.jobId} accepted`, 'OK', { duration: 2500 });
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
