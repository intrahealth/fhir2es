const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '60000/1m',
  ESMaxScrollContext: '60000',
  FHIRBaseURL: 'http://localhost:8081/hapi/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  relationshipsIDs: [], //if not specified then all relationships will be processed
  reset: false
})
caching.cache().then(() => {
  console.log('Done')
})
