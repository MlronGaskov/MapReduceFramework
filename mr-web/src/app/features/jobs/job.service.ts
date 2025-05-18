import { HttpClient } from "@angular/common/http";
import { inject, Injectable } from "@angular/core";
import { map, Observable } from "rxjs";

import { JobSummary } from './job-list/job-list-item/job-list-item.component';

export type PhaseName = 'MAP' | 'REDUCE';
export type JobStatus = 'RUNNING' | 'FINISHED' | 'WAITING';
export type TerminationStatus = 'OK' | 'ABORTED';

export interface PhaseDuration {
  phaseName: 'MAP' | 'REDUCE';
  start: string;
  end: string;
}

export interface JobProgress {
  status: 'RUNNING' | 'FINISHED';
  phase: PhaseName;

  totalTasks: number;
  completedTasks: number;

  terminationStatus: TerminationStatus;
  phaseDurations: PhaseDuration[];  
} 

export interface JobInfo {
  jobName: string;

  jobStorageType: string;
  dataStorageType: string;

  inputsPath: string;
  outputsPath: string; 

  mappers:  number;
  reducers: number;

  progressInfo: JobProgress;
}

export interface UploadJobRequest {
  jobId: number;
  jobName: string;
  jobPath: string;

  jobStorageConnectionString: string;
  dataStorageConnectionString: string;

  inputsPath: string;
  mappersOutputsPath: string;
  reducersOutputsPath: string;

  mappersCount: number;
  reducersCount: number;
  sorterInMemoryRecords: number;
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
  return this.http.delete(
    this.api(`/jobs/${id}`),
    { responseType: 'text' }
  ).pipe(map(() => void 0));
}

  getJobInfo(id: number): Observable<JobInfo> {
    return this.http.get<JobInfo>(this.api(`/jobs/${id}`));
  }

  getProgress(id: number): Observable<JobProgress> {
    return this.http.get<JobProgress>(this.api(`/jobs/${id}/progress`));
  }

  uploadJob(req: UploadJobRequest): Observable<string> {
    return this.http.put(
      this.api('/job'),
      req,
      { responseType: 'text' }
    );
  }
}