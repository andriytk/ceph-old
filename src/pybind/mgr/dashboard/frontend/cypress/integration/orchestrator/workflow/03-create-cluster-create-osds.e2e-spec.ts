import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';
import { OSDsPageHelper } from 'cypress/integration/cluster/osds.po';

const osds = new OSDsPageHelper();

describe('Create cluster create osds page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
    cy.get('.nav-link').contains('Create OSDs').click();
  });

  it('should check if title contains Create OSDs', () => {
    cy.get('.title').should('contain.text', 'Create OSDs');
  });

  describe('when Orchestrator is available', () => {
    it('should create OSDs', () => {
      osds.navigateTo();
      osds.getTableCount('total').as('initOSDCount');

      createCluster.navigateTo();
      createCluster.createCluster();
      cy.get('.nav-link').contains('Create OSDs').click();

      createCluster.createOSD('hdd');

      // Go to the Review section and Expand the cluster
      // because the drive group spec is only stored
      // in frontend and will be lost when refreshed
      cy.get('.nav-link').contains('Review').click();
      cy.get('button[aria-label="Next"]').click();
      cy.get('cd-dashboard').should('exist');
    });
  });
});
