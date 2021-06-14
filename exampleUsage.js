const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '100000/1m',
  ESMaxScrollContext: '100000',
  FHIRBaseURL: 'http://localhost:8081/hapi_kenya/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  since: '', //use this to specify last updated time of resources to be processed
  relationshipsIDs: ["ihris-es-report-mhero-flow-run-breakdown"], //if not specified then all relationships will be processed
  reset: false //will pull all resources if set to true
})
caching.cache().then(() => {
  console.log('Done')
})
