import promisify from 'cypress-promise';


describe("Testing metadata", () => {

  before(() => {
    cy.geonatureLogout();
    cy.geonatureLogin();
    cy.visit("/#/occhab")
  });

  it('should create an habitation', () => {
    const canvas = "body > pnx-root > pnx-nav-home > mat-sidenav-container > mat-sidenav-content > div > div > pnx-occhab-form > div > div:nth-child(1) > pnx-map > div > div.leaflet-container.leaflet-touch.leaflet-fade-anim.leaflet-grab.leaflet-touch-drag.leaflet-touch-zoom"
    cy.get('#add-btn').click()

    cy.get('#validateButton').should('be.disabled')

    cy.get('body > pnx-root > pnx-nav-home > mat-sidenav-container > mat-sidenav-content > div > div > pnx-occhab-form > div > div:nth-child(1) > pnx-map > div > div.leaflet-container.leaflet-touch.leaflet-fade-anim.leaflet-grab.leaflet-touch-drag.leaflet-touch-zoom > div.leaflet-control-container > div.leaflet-top.leaflet-left > div.leaflet-draw.leaflet-control > div:nth-child(1) > div > a').click()
    cy.get(canvas).click(250,250)
    cy.get(canvas).click(300,250)
    cy.get(canvas).click(300,300)
    cy.get(canvas).click(250,300)
    cy.get(canvas).click(250,250)
    cy.get('#validateButton').should('be.disabled')
    
    cy.get('[data-qa="gn-common-form-observers-select"]').click()
    cy.get('[data-qa="gn-common-form-observers-select-AGENT test"]').click()
    cy.get('#validateButton').should('be.disabled')
    
    cy.get('#station-card > div > div:nth-child(1) > div.col > pnx-datasets > ng-select').click()
    cy.get('[data-qa="Carto d\'habitat X"]').click()
    cy.get('#validateButton').should('be.disabled')
    
    cy.get('#station-card > div > div:nth-child(7) > div > pnx-dumb-select > div > select').select("1: Object")
    cy.get('#validateButton').should('be.disabled')

    cy.get('#add-hab-btn').click()
    cy.get('#taxonInput').type('dune')
    cy.get('#ngb-typeahead-3-0').click()
    cy.get('body > pnx-root > pnx-nav-home > mat-sidenav-container > mat-sidenav-content > div > div > pnx-occhab-form > div > div:nth-child(2) > div > div:nth-child(3) > div > div > pnx-dumb-select:nth-child(4) > div > select').select('1: Object')
    cy.get('body > pnx-root > pnx-nav-home > mat-sidenav-container > mat-sidenav-content > div > div > pnx-occhab-form > div > div:nth-child(2) > div > div:nth-child(3) > div > div > button').click()

    cy.get('#validateButton').click()

  })

  it('should display the new habitation', async () => {
    const listHabit = await promisify(cy.get('body > pnx-root > pnx-nav-home > mat-sidenav-container > mat-sidenav-content > div > div > pnx-occhab-map-list > div.row.row-sm.map-list-container > div:nth-child(2) > ngx-datatable > div > datatable-body > datatable-selection > datatable-scroller'))
    expect(listHabit[0].children[0].children[0].children[1].children[4].innerText).contains('Prés salés du contact haut schorre/dune')
    listHabit[0].children[0].children[0].children[1].children[2].children[0].children[0].click()
  })

})