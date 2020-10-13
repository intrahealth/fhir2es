const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '10000/1m',
  FHIRBaseURL: 'http://localhost:8081/hapi4/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  relationshipsIDs: ["ihris-es-report-mhero-send-message"], //if not specified then all relationships will be processed
  reset: true
})
caching.cache().then(() => {
  console.log('Done')
})
