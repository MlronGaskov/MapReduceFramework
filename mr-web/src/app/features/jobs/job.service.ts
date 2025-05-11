import { HttpClient } from "@angular/common/http";
import { inject, Injectable } from "@angular/core";
import { Observable } from "rxjs";

import { JobSummary } from './job-list/job-list-item/job-list-item.component';

export interface JobInfo {
  id: number;
  name: string;

  jobStorageType:  'LOCAL' | 'CEPH' | 'S3' | 'YANDEX_CLOUD';
  dataStorageType: 'LOCAL' | 'CEPH' | 'S3' | 'YANDEX_CLOUD';

  inputsPath: string;
  outputsPath: string; 

  mappers:  number;
  reducers: number;

  createdAt: string;
}

export interface JobProgress {
  status:  'RUNNING' | 'FINISHED' | 'WAITING';
  phase:   'MAP' | 'REDUCE' | 'FINISHED';

  total: number;
  completed: number;
}

export interface UploadJobRequest {
  jobName: string;
  jobUrl: string;
}


@Injectable({ providedIn: "root" })
export class JobService {
  http = inject(HttpClient);
  private coordinatorUrl = ''; 

  setCoordinatorUrl(url: string): void {
    this.coordinatorUrl = url.replace(/\/+$/, ''); // remove endings «/»
  }

  private api(path: string): string {
    if (!this.coordinatorUrl) {
      throw new Error('Coordinator URL not set');
    }
    return `${this.coordinatorUrl}${path}`;
  }

  getJobs(): Observable<JobSummary[]> {
    return this.http.get<JobSummary[]>(this.api('/jobs'));
  }

  deleteJob(id: number): Observable<void> {
    return this.http.delete<void>(this.api(`/jobs/${id}`));
  }

  getJobInfo(id: number): Observable<JobInfo> {
    return this.http.get<JobInfo>(this.api(`/jobs/${id}`));
  }

  getProgress(id: number): Observable<JobProgress> {
    return this.http.get<JobProgress>(this.api(`/jobs/${id}/progress`));
  }

  uploadJob(req: UploadJobRequest): Observable<{ jobId: number; uiUrl: string }> {
    return this.http.post<{ jobId: number; uiUrl: string }>(this.api('/jobs'), req);
  }
}