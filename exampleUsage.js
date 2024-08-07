const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '100000/1m',
  ESMaxScrollContext: '100000',
  FHIRBaseURL: 'http://localhost:8081/chad/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  since: '', //use this to specify last updated time of resources to be processed
  relationshipsIDs: ['ihris-es-report-chad-staff-directorate'], //if not specified then all relationships will be processed
  reset: true, //will pull all resources if set to true
  ESModulesBasePath: "/home/ally/iHRIS/ihris-backend/namibia/modules/es"
})
caching.cache().then(() => {
  console.log('Done')
})
