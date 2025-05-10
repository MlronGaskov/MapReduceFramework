import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JobUploaderComponent } from './job-uploader.component';

describe('JobUploaderComponent', () => {
  let component: JobUploaderComponent;
  let fixture: ComponentFixture<JobUploaderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [JobUploaderComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(JobUploaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
