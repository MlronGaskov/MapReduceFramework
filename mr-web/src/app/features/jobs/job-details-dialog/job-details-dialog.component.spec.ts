import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JobDetailsDialogComponent } from './job-details-dialog.component';

describe('JobDetailsDialogComponent', () => {
  let component: JobDetailsDialogComponent;
  let fixture: ComponentFixture<JobDetailsDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [JobDetailsDialogComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(JobDetailsDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
