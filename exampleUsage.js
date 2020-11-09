const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '60000/1m',
  ESMaxScrollContext: '60000',
  FHIRBaseURL: 'http://localhost:8081/hapi4/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  since: '2020-11-07T09:29:00', //use this to specify last updated time of resources to be processed
  relationshipsIDs: [], //if not specified then all relationships will be processed
  reset: true //will pull all resources if set to true
})
caching.cache().then(() => {
  console.log('Done')
})
