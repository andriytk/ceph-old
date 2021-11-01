import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { PerformanceCounterModule } from '~/app/ceph/performance-counter/performance-counter.module';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, TabHelper } from '~/testing/unit-test-helper';
import { RgwDaemonDetailsComponent } from '../rgw-daemon-details/rgw-daemon-details.component';
import { RgwDaemonListComponent } from './rgw-daemon-list.component';

describe('RgwDaemonListComponent', () => {
  let component: RgwDaemonListComponent;
  let fixture: ComponentFixture<RgwDaemonListComponent>;
  let getPermissionsSpy: jasmine.Spy;
  let getRealmsSpy: jasmine.Spy;
  const permissions = new Permissions({ grafana: ['read'] });

  const expectTabsAndHeading = (length: number, heading: string) => {
    const tabs = TabHelper.getTextContents(fixture);
    expect(tabs.length).toEqual(length);
    expect(tabs[length - 1]).toEqual(heading);
  };

  configureTestBed({
    declarations: [RgwDaemonListComponent, RgwDaemonDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      NgbNavModule,
      PerformanceCounterModule,
      SharedModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    getPermissionsSpy = spyOn(TestBed.inject(AuthStorageService), 'getPermissions');
    getPermissionsSpy.and.returnValue(new Permissions({}));
    getRealmsSpy = spyOn(TestBed.inject(RgwSiteService), 'get');
    getRealmsSpy.and.returnValue(of([]));
    fixture = TestBed.createComponent(RgwDaemonListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should only show Daemons List tab', () => {
    fixture.detectChanges();

    expectTabsAndHeading(1, 'Daemons List');
  });

  it('should show Overall Performance tab', () => {
    getPermissionsSpy.and.returnValue(permissions);
    fixture.detectChanges();

    expectTabsAndHeading(2, 'Overall Performance');
  });

  it('should show Sync Performance tab', () => {
    getPermissionsSpy.and.returnValue(permissions);
    getRealmsSpy.and.returnValue(of(['realm1']));
    fixture.detectChanges();

    expectTabsAndHeading(3, 'Sync Performance');
  });
});
