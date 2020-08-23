const env = process.env.NODE_ENV || 'development';
const { CacheFhirToES } = require('./reports')

let caching = new CacheFhirToES({
  ESBaseURL: 'http://localhost:9200',
  ESUsername: '',
  ESPassword: '',
  ESMaxCompilationRate: '10000/1m',
  FHIRBaseURL: 'http://localhost:8081/scratchpad/fhir',
  FHIRUsername: '',
  FHIRPassword: ''
})
caching.cache()
