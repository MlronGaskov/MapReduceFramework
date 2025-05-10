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
  status:  'RUNNING' | 'SUCCEEDED' | 'FAILED';
  phase:   'MAP' | 'REDUCE' | 'FINISHED';

  total: number;
  completed: number;
}

export interface UploadJobRequest {
  jobName: string;
  userJar: string;
  mainClass: string;
  mappers: number;
  reducers: number;
  inputsPath: string;
  outputsPath: string;

  jobStorageType?:  'LOCAL' | 'CEPH' | 'S3' | 'YANDEX_CLOUD';
  dataStorageType?: 'LOCAL' | 'CEPH' | 'S3' | 'YANDEX_CLOUD';
}


@Injectable({ providedIn: "root" })
export class JobService {
  private readonly api = "192.168.10.10:8080";

  http = inject(HttpClient);

  //constructor(private readonly http: HttpClient) {}

  getJobs(): Observable<JobSummary[]> {
    return this.http.get<JobSummary[]>(`${this.api}/jobs`);
  }

  deleteJob(id: number): Observable<void> {
    return this.http.delete<void>(`${this.api}/jobs/${id}`);
  }

  getJobInfo(id: number): Observable<JobInfo> {
    return this.http.get<JobInfo>(`${this.api}/jobs/${id}`);
  }

  getProgress(id: number): Observable<JobProgress> {
    return this.http.get<JobProgress>(`${this.api}/jobs/${id}/progress`);
  }

  uploadJob(req: UploadJobRequest): Observable<{ jobId: number; uiUrl: string }> {
    return this.http.post<{ jobId: number; uiUrl: string }>(`${this.api}/jobs`, req);
  }
}