"use strict";
const axios = require('axios');
const async = require('async');
const moment = require('moment')
const logger = require('./winston')
const URI = require('urijs');
const _ = require('lodash');
const fhirpath = require('fhirpath');
class CacheFhirToES {
  constructor({
    ESBaseURL,
    ESUsername,
    ESPassword,
    ESMaxCompilationRate,
    ESMaxScrollContext,
    FHIRBaseURL,
    FHIRUsername,
    FHIRPassword,
    relationshipsIDs = [],
    since,
    reset = false
  }) {
    this.ESBaseURL = ESBaseURL
    this.ESUsername = ESUsername
    this.ESPassword = ESPassword
    this.ESMaxCompilationRate = ESMaxCompilationRate
    this.FHIRBaseURL = FHIRBaseURL
    this.FHIRUsername = FHIRUsername
    this.FHIRPassword = FHIRPassword
    this.relationshipsIDs = relationshipsIDs
    this.ESMaxScrollContext = ESMaxScrollContext
    this.since = since
    this.reset = reset
  }

  flattenComplex(extension) {
    let results = {};
    for (let ext of extension) {
      let value = '';
      for (let key of Object.keys(ext)) {
        if (key !== 'url') {
          value = ext[key];
        }
      }
      if (results[ext.url]) {
        if (Array.isArray(results[ext.url])) {
          results[ext.url].push(value);
        } else {
          results[ext.url] = [results[ext.url], value];
        }
      } else {
        if (Array.isArray(value)) {
          results[ext.url] = [value];
        } else {
          results[ext.url] = value;
        }
      }
    }
    return results;
  };

  /**
   *
   * @param {relativeURL} reference //reference must be a relative url i.e Practioner/10
   */
  getResourceFromReference(reference) {
    return new Promise((resolve) => {
      let url = URI(this.FHIRBaseURL)
        .segment(reference)
        .toString()
      axios.get(url, {
        withCredentials: true,
        auth: {
          username: this.FHIRUsername,
          password: this.FHIRPassword
        },
      }).then(response => {
        logger.info('sending back response');
        return resolve(response.data)
      }).catch((err) => {
        logger.error('Error occured while getting resource reference');
        logger.error(err);
        return resolve()
      })
    }).catch((err) => {
      logger.error('Error occured while getting resource reference');
      logger.error(err);
    })
  }

  /**
   *
   * @param {Array} extension
   * @param {String} element
   */
  getElementValFromExtension(extension, element) {
    return new Promise((resolve) => {
      let elementValue = ''
      async.each(extension, (ext, nxtExt) => {
        let value
        for (let key of Object.keys(ext)) {
          if (key !== 'url') {
            value = ext[key];
          }
        }
        if (ext.url === element) {
          elementValue = value
        }
        (async () => {
          if (Array.isArray(value)) {
            let val
            try {
              val = await this.getElementValFromExtension(value, element)
            } catch (error) {
              logger.error(error);
            }
            if (val) {
              elementValue = val
            }
            return nxtExt()
          } else {
            return nxtExt()
          }
        })();
      }, () => {
        resolve(elementValue)
      })
    }).catch((err) => {
      logger.error('Error occured while geting Element value from extension');
      logger.error(err);
    })
  }

  getImmediateLinks(links, callback) {
    if (this.orderedResources.length - 1 === links.length) {
      return callback(this.orderedResources);
    }
    let promises = [];
    for (let link of links) {
      promises.push(
        new Promise((resolve, reject) => {
          link = this.flattenComplex(link.extension);
          let parentOrdered = this.orderedResources.find(orderedResource => {
            let linkToResource = link.linkTo.split('.').shift()
            return orderedResource.name === linkToResource;
          });
          let exists = this.orderedResources.find(orderedResource => {
            return JSON.stringify(orderedResource) === JSON.stringify(link);
          });
          if (parentOrdered && !exists) {
            this.orderedResources.push(link);
          }
          resolve();
        })
      );
    }
    Promise.all(promises).then(() => {
      if (this.orderedResources.length - 1 !== links.length) {
        this.getImmediateLinks(links, () => {
          return callback();
        });
      } else {
        return callback();
      }
    });
  };

  getReportRelationship(callback) {
    let url = URI(this.FHIRBaseURL)
      .segment('Basic')
      .addQuery('code', 'iHRISRelationship');
    for(let relationship of this.relationshipsIDs) {
      url.addQuery('_id', relationship);
    }
    url = url.toString();
    axios
      .get(url, {
        withCredentials: true,
        auth: {
          username: this.FHIRUsername,
          password: this.FHIRPassword,
        },
      })
      .then(relationships => {
        return callback(false, relationships.data);
      })
      .catch(err => {
        logger.error(err);
        return callback(err, false);
      });
  };

  updateESScrollContext() {
    return new Promise((resolve, reject) => {
      logger.info('Setting maximum open scroll context');
      let url = URI(this.ESBaseURL).segment('_cluster').segment('settings').toString();
      let body = {
        "persistent": {
          "search.max_open_scroll_context": this.ESMaxScrollContext
        },
        "transient": {
          "search.max_open_scroll_context": this.ESMaxScrollContext
        }
      };
      axios({
          method: 'PUT',
          url,
          auth: {
            username: this.ESUsername,
            password: this.ESPassword,
          },
          data: body
        })
        .then(response => {
          if (response.status > 199 && response.status < 299) {
            logger.info('maximum open scroll context updated successfully');
            return resolve()
          } else {
            logger.error('An error has occured while setting max open scroll context')
            return reject()
          }
        }).catch((err) => {
          logger.error('An error has occured while setting max open scroll context')
          reject()
          throw err
        })
    })
  }

  updateESCompilationsRate(callback) {
    logger.info('Setting maximum compilation rate');
    let url = URI(this.ESBaseURL).segment('_cluster').segment('settings').toString();
    let body = {
      "transient": {
        "script.max_compilations_rate": this.ESMaxCompilationRate
      }
    };
    axios({
        method: 'PUT',
        url,
        auth: {
          username: this.ESUsername,
          password: this.ESPassword,
        },
        data: body
      })
      .then(response => {
        if (response.status > 199 && response.status < 299) {
          logger.info('maximum compilation rate updated successfully');
          return callback(false)
        } else {
          logger.error('An error has occured while setting max compilation rate')
          return callback(true)
        }
      }).catch((err) => {
        logger.error('An error has occured while setting max compilation rate')
        callback(err)
        throw err
      })
  }

  updateLastIndexingTime(time, index) {
    return new Promise((resolve, reject) => {
      logger.info('Updating lastIndexingTime')
      axios({
        url: URI(this.ESBaseURL).segment('syncdata').segment("_doc").segment(index).toString(),
        method: 'PUT',
        auth: {
          username: this.ESUsername,
          password: this.ESPassword
        },
        data: {
          "lastIndexingTime": time
        }
      }).then((response) => {
        if(response.status < 200 && response.status > 299) {
          logger.error('An error occured while updating lastIndexingTime')
          return reject()
        }
        return resolve(false)
      }).catch((err) => {
        logger.error(err)
        logger.error('An error occured while updating lastIndexingTime')
        return reject(true)
      })
    })
  }

  getLastIndexingTime(index) {
    return new Promise((resolve, reject) => {
      if(this.since && !this.reset) {
        this.lastIndexingTime = this.since
        return resolve()
      }
      logger.info('Getting lastIndexingTime')
      let query = {
        query: {
          term: {
            _id: index
          }
        }
      }
      axios({
        method: "GET",
        url: URI(this.ESBaseURL).segment('syncdata').segment("_search").toString(),
        data: query,
        auth: {
          username: this.ESUsername,
          password: this.ESPassword
        }
      }).then((response) => {
        if(this.reset) {
          logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
          this.lastIndexingTime = '1970-01-01T00:00:00'
          return resolve()
        }
        if(response.data.hits.hits.length === 0) {
          logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
          this.lastIndexingTime = '1970-01-01T00:00:00'
          return resolve()
        }
        logger.info('Returning lastIndexingTime of ' + response.data.hits.hits[0]._source.lastIndexingTime)
        this.lastIndexingTime = response.data.hits.hits[0]._source.lastIndexingTime
        return resolve()
      }).catch((err) => {
        if (err.response && err.response.status && err.response.status === 404) {
          this.lastIndexingTime = '1970-01-01T00:00:00'
          logger.info('Index not found, creating index syncData');
          let mappings = {
            mappings: {
              properties: {
                lastIndexingTime: {
                  type: "text"
                }
              },
            },
          };
          axios({
              method: 'PUT',
              url: URI(this.ESBaseURL).segment('syncdata').toString(),
              data: mappings,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              }
            })
            .then(response => {
              if (response.status !== 200) {
                logger.error('Something went wrong and index was not created');
                logger.error(response.data);
                logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
                return reject()
              } else {
                logger.info('Index syncdata created successfully');
                logger.info('Adding default lastIndexTime which is 1970-01-01T00:00:00')
                axios({
                  method: 'PUT',
                  auth: {
                    username: this.ESUsername,
                    password: this.ESPassword,
                  },
                  url: URI(this.ESBaseURL).segment('syncdata').segment("_doc").segment(index).toString(),
                  data: {
                    "lastIndexingTime": "1970-01-01T00:00:00"
                  }
                }).then((response) => {
                  if(response.status >= 200 && response.status <= 299) {
                    logger.info('Default lastIndexTime added')
                  } else {
                    logger.error('An error has occured while saving default lastIndexTime');
                    return reject()
                  }
                  logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
                  return resolve()
                }).catch((err) => {
                  logger.error('An error has occured while saving default lastIndexTime');
                  if (err.response && err.response.data) {
                    logger.error(err.response.data);
                  }
                  if (err.error) {
                    logger.error(err.error);
                  }
                  if (!err.response) {
                    logger.error(err);
                  }
                  return reject()
                })
              }
            })
            .catch(err => {
              logger.error('Error: ' + err);
              logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
              return reject()
            });
        } else {
          logger.error('Error occured while creating ES index');
          logger.error(err);
          logger.info('Returning lastIndexingTime of 1970-01-01T00:00:00')
          this.lastIndexingTime = '1970-01-01T00:00:00'
          return reject()
        }
      })
    })
  }

  createESIndex(name, IDFields, callback) {
    logger.info('Checking if index ' + name + ' exists');
    let url = URI(this.ESBaseURL)
      .segment(name.toString().toLowerCase())
      .toString();
    axios({
        method: 'head',
        url,
        auth: {
          username: this.ESUsername,
          password: this.ESPassword,
        },
      })
      .then(response => {
        if (response.status === 200) {
          logger.info('Index ' + name + ' exist, not creating');
          return callback(false);
        } else {
          return callback(true);
        }
      })
      .catch(err => {
        if (err.response && err.response.status && err.response.status === 404) {
          logger.info('Index not found, creating index ' + name);
          let mappings = {
            mappings: {
              properties: {},
            },
          };
          for (let IDField of IDFields) {
            mappings.mappings.properties[IDField] = {};
            mappings.mappings.properties[IDField].type = 'text';
            mappings.mappings.properties[IDField].fields = {
              keyword: {
                type: 'keyword'
              }
            }
          }
          axios({
              method: 'put',
              url: url,
              data: mappings,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              },
            })
            .then(response => {
              if (response.status !== 200) {
                logger.error('Something went wrong and index was not created');
                logger.error(response.data);
                return callback(true);
              } else {
                logger.info('Index ' + name + ' created successfully');
                return callback(false);
              }
            })
            .catch(err => {
              logger.error(err);
              return callback(true);
            });
        } else {
          logger.error('Error occured while creating ES index');
          logger.error(err);
          return callback(true);
        }
      });
  };

  refreshIndex (index) {
    return new Promise((resolve, reject) => {
      logger.info('Refreshing index ' + index);
      const url = URI(this.ESBaseURL)
        .segment(index)
        .segment('_refresh')
        .toString();
      axios({
        method: 'post',
        url,
        auth: {
          username: this.ESUsername,
          password: this.ESPassword,
        },
      }).then(response => {
        logger.info(`index ${index} refreshed`);
        return resolve()
      }).catch((err) => {
        if (err.response && (err.response.statusText === 'Conflict' || err.response.status === 409)) {
          logger.warn('Conflict occured, rerunning this request');
          setTimeout(() => {
            this.refreshIndex(index, (err) => {
              if(err) {
                return reject();
              }
              return resolve();
            })
          }, 2000)
        } else {
          logger.error('Error Occured while refreshing index');
          if (err.response && err.response.data) {
            logger.error(err.response.data);
          }
          if (err.error) {
            logger.error(err.error);
          }
          if (!err.response) {
            logger.error(err);
          }
          return reject();
        }
      });
    })
  };

  getResourcesFields(resources) {
    let fields = []
    for(let resource of resources) {
      if(resource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
        for (let element of resource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
          let fieldLabel
          let fhirPathValue
          let fieldAutogenerated = false
          for (let el of element) {
            let value = '';
            for (let key of Object.keys(el)) {
              if (key !== 'url') {
                value = el[key];
              }
            }
            if (el.url === "label") {
              let fleldChars = value.split(' ')
              //if label has space then format it
              if (fleldChars.length > 1) {
                fieldLabel = value.toLowerCase().split(' ').map(word => word.replace(word[0], word[0].toUpperCase())).join('');
              } else {
                fieldLabel = value
              }
            } else if (el.url === "name") {
              fhirPathValue = value
            } else if (el.url === "autoGenerated") {
              fieldAutogenerated = value
            }
          }
          fields.push({
            resourceName: resource.name,
            resourceType: resource.resource,
            field: fieldLabel,
            fhirPath: fhirPathValue,
            fieldAutogenerated
          })
        }
        fields.push({
          resourceName: resource.name,
          resourceType: resource.resource,
          field: resource.name,
          fhirPath: "id",
          fieldAutogenerated: false
        })
      }
    }
    return fields;
  }

  getChildrenResources(resourceName) {
    let childrenResources = []
    for(let orderedResource of this.orderedResources) {
      if(orderedResource.linkTo === resourceName || (orderedResource.linkTo && orderedResource.linkTo.startsWith(resourceName + '.'))) {
        childrenResources.push(orderedResource)
        let grandChildren = this.getChildrenResources(orderedResource.name)
        childrenResources = childrenResources.concat(grandChildren)
      }
    }
    return childrenResources
  }

  getESDocument(index, query, callback) {
    let error = false
    let documents = []
    if(!query) {
      query = {}
    }
    query.size = 10000
    let url = URI(this.ESBaseURL)
      .segment(index)
      .segment('_search')
      .addQuery('scroll', '1m')
      .toString()
    let scroll_id = null
    async.doWhilst(
      (callback) => {
        axios({
          method: 'GET',
          url,
          data: query,
          auth: {
            username: this.ESUsername,
            password: this.ESPassword
          }
        }).then((response) => {
          if(response.data.hits.hits.length === 0 || !response.data._scroll_id) {
            scroll_id = null
          } else {
            scroll_id = response.data._scroll_id
            documents = documents.concat(response.data.hits.hits)
            url = URI(this.ESBaseURL).segment('_search').segment('scroll').toString()
            query = {
              scroll_id: scroll_id
            }
          }
          return callback(null)
        }).catch((err) => {
          error = err
          logger.error(err);
          scroll_id = null
          return callback(null)
        })
      },
      (callback) => {
        return callback(null, scroll_id !== null)
      },
      () => {
        return callback(error, documents)
      }
    )
  }

  deleteESDocument(query, index) {
    return new Promise(async(resolve, reject) => {
      // await this.refreshIndex(index);
      let url = URI(this.ESBaseURL).segment(index).segment('_delete_by_query').toString();
      axios({
        method: 'post',
        url,
        data: query,
        auth: {
          username: this.ESUsername,
          password: this.ESPassword,
        },
      }).then(response => {
        logger.info(JSON.stringify(response.data,0,2));
        return resolve()
      }).catch((err) => {
        if (err.response && (err.response.statusText === 'Conflict' || err.response.status === 409)) {
          logger.warn('Conflict occured, rerunning this request');
          setTimeout(async() => {
            await deleteESDocument(query, index).then(() => {
              return resolve()
            }).catch(() => {
              return reject()
            })
          }, 2000)
        } else {
          logger.error('Error Occured while deleting ES document');
          if (err.response && err.response.data) {
            logger.error(err.response.data);
          }
          if (err.error) {
            logger.error(err.error);
          }
          if (!err.response) {
            logger.error(err);
          }
          return reject()
        }
      });
    })
  };

  async updateESDocument(body, record, index, orderedResource, resourceData, tryDeleting, callback) {
    let multiple = orderedResource.multiple
    // await this.refreshIndex(index);
    //this handles records that should be deleted instead of its fields being truncated
    let recordDeleted = false;
    async.series({
      addNewRows: (callback) => {
        //ensure that this is not the primary resource and has multiple tag, otherwise return
        if(!orderedResource.hasOwnProperty('linkElement') || !multiple) {
          return callback(null)
        }
        let query = {
          query: {
            terms: body.query.terms
          }
        }
        this.getESDocument(index, query, async(err, documents) => {
          if(err) {
            logger.error('Req Data: ' + JSON.stringify(query,0,2));
            return callback(null)
          }
          //if field values for this record needs to be truncated and there are multiple records of the parent, then delete the one we are truncating instead of updating
          if(documents.length > 1 && tryDeleting) {
            let recordFields = Object.keys(record)
            let idField = recordFields[recordFields.length - 1]
            let termField = Object.keys(body.query.terms)[0]
            let delQry = {
              query: {
                bool: {
                  must: []
                }
              }
            }
            let must1 = {
              terms: {}
            }
            must1.terms[termField] = body.query.terms[termField]
            delQry.query.bool.must.push(must1)
            let must2 = {
              terms: {}
            }
            must2.terms[idField] = [record[idField]]
            delQry.query.bool.must.push(must2)
            try {
              await this.deleteESDocument(delQry, index)
            } catch (error) {
              logger.error(error);
            }
            recordDeleted = true;
          }
          if(recordDeleted) {
            return callback(null)
          }
          //because this resource supports multiple rows, then we are trying to add new rows
          let newRowBody = {}
          // take the last field because it is the ID
          let recordFields = Object.keys(record)
          let checkField = recordFields[recordFields.length - 1]
          for(let linkField in body.query.terms) {
            for(let index in body.query.terms[linkField]) {
              // create new row only if there is no checkField or checkField exist but it is different
              let updateThis = documents.find((hit) => {
                return hit['_source'][linkField] === body.query.terms[linkField][index] && (!hit['_source'][checkField] || hit['_source'][checkField] === record[checkField])
              })
              if(!updateThis) {
                let hit = documents.find((hit) => {
                  return hit['_source'][linkField] === body.query.terms[linkField][index]
                })
                if(!hit) {
                  continue;
                }
                for(let field in hit['_source']) {
                  newRowBody[field] = hit['_source'][field]
                }
                for(let recField in record) {
                  newRowBody[recField] = record[recField]
                }
                body.query.terms[linkField].splice(index, 1)
              }
            }
          }
          if(Object.keys(newRowBody).length > 0) {
            let url = URI(this.ESBaseURL).segment(index).segment('_doc').toString()
            axios({
              method: 'POST',
              url,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              },
              data: newRowBody
            }).then((response) => {
              return callback(null)
            }).catch((err) => {
              logger.error(err);
              logger.error('Req Data: ' + JSON.stringify(newRowBody,0,2));
              return callback(null)
            })
          } else {
            return callback(null)
          }
        })
      },
      updateRow: (callback) => {
        if(recordDeleted) {
          return callback(null);
        }
        // for multiple rows, ensure that we dont update all rows but just one row
        let bodyData = {}
        let recordFields = Object.keys(record)
        let idField = recordFields[recordFields.length - 1]
        if(multiple) {
          let termField = Object.keys(body.query.terms)[0]
          bodyData = {
            query: {
              bool: {
                must: []
              }
            }
          }
          let must1 = {
            terms: {}
          }
          must1.terms[termField] = body.query.terms[termField]
          bodyData.query.bool.must.push(must1)
          let must2 = {
            terms: {}
          }
          must2.terms[idField] = [record[idField]]
          bodyData.query.bool.must.push(must2)
          bodyData.script = body.script
        } else {
          bodyData = body
        }
        let url = URI(this.ESBaseURL).segment(index).segment('_update_by_query').toString();
        async.series({
          updateDocMissingField: (callback) => {
            if(!multiple) {
              return callback(null)
            }
            let updBodyData = _.cloneDeep(bodyData)
            updBodyData.query.bool.must.splice(1, 1)
            updBodyData.query.bool.must_not = {
              exists: {}
            }
            updBodyData.query.bool.must_not.exists.field = idField
            axios({
              method: 'post',
              url,
              data: updBodyData,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              },
            }).then(response => {
              return callback(null)
            }).catch(err => {
              if (err.response && (err.response.statusText === 'Conflict' || err.response.status === 409)) {
                logger.warn('Conflict occured, rerunning this request');
                setTimeout(() => {
                  this.updateESDocument(body, record, index, orderedResource, resourceData, tryDeleting, () => {
                    return callback(null)
                  })
                }, 2000)
              } else {
                logger.error('Error Occured while creating ES document');
                if (err.response && err.response.data) {
                  logger.error(err.response.data);
                }
                if (err.error) {
                  logger.error(err.error);
                }
                if (!err.response) {
                  logger.error(err);
                }
                logger.error('Req Data: ' + JSON.stringify(updBodyData,0,2));
                return callback(null)
              }
            });
          },
          updateDocHavingField: (callback) => {
            axios({
              method: 'post',
              url,
              data: bodyData,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              },
            }).then(response => {
              // if nothing was updated and its from the primary (top) resource then create as new
              if (response.data.updated == 0 && !orderedResource.hasOwnProperty('linkElement')) {
                logger.info('No record with id ' + resourceData.id + ' found on elastic search, creating new');
                let url = URI(this.ESBaseURL)
                  .segment(index)
                  .segment('_doc')
                  .toString();
                axios({
                    method: 'post',
                    url,
                    data: record,
                    auth: {
                      username: this.ESUsername,
                      password: this.ESPassword,
                    },
                  })
                  .then(response => {
                    return callback(null)
                  })
                  .catch(err => {
                    logger.error('Error occured while saving document into ES');
                    logger.error(err);
                    logger.error('Req Data: ' + JSON.stringify(record,0,2));
                    return callback(null)
                  });
              } else {
                return callback(null)
              }
            }).catch(err => {
              if (err.response && (err.response.statusText === 'Conflict' || err.response.status === 409)) {
                logger.warn('Conflict occured, rerunning this request');
                setTimeout(() => {
                  this.updateESDocument(body, record, index, orderedResource, resourceData, tryDeleting, () => {
                    return callback(null)
                  })
                }, 2000)
              } else {
                logger.error('Error Occured while creating ES document');
                if (err.response && err.response.data) {
                  logger.error(err.response.data);
                }
                if (err.error) {
                  logger.error(err.error);
                }
                if (!err.response) {
                  logger.error(err);
                }
                logger.error('Req Data: ' + JSON.stringify(bodyData,0,2));
                return callback(null)
              }
            });
          }
        }, () => {
          return callback(null)
        })
      },
      cleanBrokenLinks: (callback) => {
        if(!orderedResource.hasOwnProperty('linkElement') || resourceData.meta.versionId == '1') {
          return callback(null)
        }
        // get all documents that doesnt meet the search terms but are linked to this resource and truncate
        let qry = {
          query: {
            bool: {
              must_not: [],
              must: []
            }
          }
        }
        qry.query.bool.must_not = [{
          terms: body.query.terms
        }]
        let recordFields = Object.keys(record)
        let idField = recordFields[recordFields.length - 1]
        let term = {}
        term[idField] = record[idField]
        qry.query.bool.must = [{
          term
        }]
        let childrenResources = this.getChildrenResources(orderedResource.name);
        childrenResources.unshift(orderedResource)
        let fields = this.getResourcesFields(childrenResources)
        let ctx = ''
        for(let field of fields) {
          ctx += 'ctx._source.' + field.field + "=null;";
        }
        this.getESDocument(index, qry, (err, documents) => {
          if(err) {
            logger.error(err);
          }
          let ids = []
          for(let hit of documents) {
            ids.push(hit._id)
          }
          if(ids.length > 0) {
            let body = {
              script: {
                lang: 'painless',
                source: ctx
              },
              query: {
                terms: {
                  _id: ids
                }
              },
            };
            let url = URI(this.ESBaseURL).segment(index).segment('_update_by_query').addQuery('conflicts', 'proceed').toString();
            axios({
              method: 'post',
              url,
              data: body,
              auth: {
                username: this.ESUsername,
                password: this.ESPassword,
              },
            }).then(response => {
              return callback(null)
            }).catch(err => {
              if (err.response && (err.response.statusText === 'Conflict' || err.response.status === 409)) {
                logger.warn('Conflict occured, rerunning this request');
                setTimeout(() => {
                  logger.error('rerun on conflict is not yet implemented');
                  return callback(null)
                }, 2000)
              } else {
                logger.error('Error Occured while truncating ES documents');
                if (err.response && err.response.data) {
                  logger.error(err.response.data);
                }
                if (err.error) {
                  logger.error(err.error);
                }
                if (!err.response) {
                  logger.error(err);
                }
                return callback(null)
              }
            })
          } else {
            return callback(null)
          }
        })
      }
    }, () => {
      return callback()
    })
  }

  cache() {
    return new Promise(async(resolve) => {
      this.getReportRelationship((err, relationships) => {
        if (err) {
          return;
        }
        if ((!relationships.entry || !Array.isArray(relationships.entry)) && !relationships.resourceType === 'Bundle') {
          logger.error('invalid resource returned');
          return;
        }
        async.eachSeries(relationships.entry, (relationship, nxtRelationship) => {
          logger.info('processing relationship ID ' + relationship.resource.id);
          relationship = relationship.resource;
          let details = relationship.extension.find(ext => ext.url === 'http://ihris.org/fhir/StructureDefinition/iHRISReportDetails');
          let links = relationship.extension.filter(ext => ext.url === 'http://ihris.org/fhir/StructureDefinition/iHRISReportLink');
          let reportDetails = this.flattenComplex(details.extension);
          let IDFields = [];
          for (let linkIndex1 in links) {
            let link1 = links[linkIndex1];
            let flattenedLink1 = this.flattenComplex(link1.extension);
            let linkTo1 = flattenedLink1.linkTo.split('.')
            let linkToResource1 = linkTo1[0]
            if (linkToResource1 === reportDetails.name) {
              let name
              if (linkTo1.length === 1) {
                name = 'id'
              } else {
                linkTo1.splice(0, 1)
                name = linkTo1.join('.')
              }
              details.extension.push({
                url: 'http://ihris.org/fhir/StructureDefinition/iHRISReportElement',
                extension: [{
                    url: 'label',
                    valueString: '__' + flattenedLink1.name + '_link',
                  },
                  {
                    url: 'name',
                    valueString: name,
                  },
                  {
                    url: 'autoGenerated',
                    valueBoolean: true
                  }
                ],
              });
              IDFields.push('__' + flattenedLink1.name + '_link')
            }

            IDFields.push(flattenedLink1.name);
            for (let link2 of links) {
              let flattenedLink2 = this.flattenComplex(link2.extension);
              let linkTo2 = flattenedLink2.linkTo.split('.')
              let linkToResource2 = linkTo2[0]
              if (linkToResource2 === flattenedLink1.name) {
                let name
                if (linkTo2.length === 1) {
                  name = 'id'
                } else {
                  linkTo2.splice(0, 1)
                  name = linkTo2.join('.')
                }
                links[linkIndex1].extension.push({
                  url: 'http://ihris.org/fhir/StructureDefinition/iHRISReportElement',
                  extension: [{
                      url: 'label',
                      valueString: '__' + flattenedLink2.name + '_link',
                    },
                    {
                      url: 'name',
                      valueString: name,
                    },
                    {
                      url: 'autoGenerated',
                      valueBoolean: true
                    }
                  ],
                });
                IDFields.push('__' + flattenedLink2.name + '_link')
              }
            }
          }
          reportDetails = this.flattenComplex(details.extension);
          this.orderedResources = [];
          // reportDetails.resource = subject._type;
          this.orderedResources.push(reportDetails);
          IDFields.push(reportDetails.name);
          this.getLastIndexingTime(reportDetails.name).then(() => {
            let newLastIndexingTime = moment()
              .subtract('1', 'minutes')
              .format('Y-MM-DDTHH:mm:ss');
            this.updateESScrollContext().then(() => {
              this.updateESCompilationsRate(() => {
                this.createESIndex(reportDetails.name, IDFields, err => {
                  if (err) {
                    logger.error('Stop creating report due to error in creating index');
                    return nxtRelationship();
                  }
                  logger.info('Done creating ES Index');
                  this.getImmediateLinks(links, () => {
                    async.eachSeries(this.orderedResources, (orderedResource, nxtResourceType) => {
                      let processedRecords = []
                      this.count = 1;
                      let offset = 0
                      let url = URI(this.FHIRBaseURL)
                        .segment(orderedResource.resource)
                        .segment('_history')
                        .addQuery('_since', this.lastIndexingTime)
                        .addQuery('_count', 200)
                        .toString();
                      logger.info(`Processing data for resource ${orderedResource.name}`);
                      async.whilst(
                        callback => {
                          return callback(null, url != false);
                        },
                        callback => {
                          axios.get(url, {
                            withCredentials: true,
                            auth: {
                              username: this.FHIRUsername,
                              password: this.FHIRPassword,
                            },
                          }).then(response => {
                            this.totalResources = response.data.total;
                            url = false;
                            const next = response.data.link.find(
                              link => link.relation === 'next'
                            );
                            if (next) {
                              url = next.url
                            }
                            if (response.data.total > 0 && response.data.entry && response.data.entry.length > 0) {
                              //if hapi server doesnt have support for returning the next cursor then use _getpagesoffset
                              offset += 200
                              if(offset <= this.totalResources && !url) {
                                url = URI(this.FHIRBaseURL)
                                .segment(orderedResource.resource)
                                .segment('_history')
                                .addQuery('_since', this.lastIndexingTime)
                                .addQuery('_count', 200)
                                .addQuery('_getpagesoffset', offset)
                                .toString();
                              }
                              this.processResource(response.data.entry, orderedResource, reportDetails, processedRecords, () => {
                                return callback(null, url);
                              })
                            } else {
                              return callback(null, url);
                            }
                          }).catch(err => {
                            logger.error('Error occured while getting resource data');
                            logger.error(err);
                            return callback(null, false)
                          });
                        }, async() => {
                          try {
                            await this.refreshIndex(reportDetails.name);
                          } catch (error) {
                            logger.error(error);
                          }
                          logger.info('Done Writting resource data for resource ' + orderedResource.name + ' into elastic search');
                          this.fixDataInconsistency(reportDetails, orderedResource, () => {
                            return nxtResourceType()
                          })
                        }
                      );
                    }, () => {
                      try {
                        this.updateLastIndexingTime(newLastIndexingTime, reportDetails.name)
                      } catch (error) {
                        logger.error(error);
                      }
                      return nxtRelationship();
                    });
                  });
                });
              })
            }).catch((err) => {
              logger.error(err);
            })
          }).catch((err) => {
            logger.error(err);
          })
        }, () => {
          logger.info('Done processing all relationships');
          return resolve()
        });
      });
    })
  }

  processResource(resourceData, orderedResource, reportDetails, processedRecords, callback) {
    async.each(resourceData, (data, nxtResource) => {
      logger.info('processing ' + this.count + '/' + this.totalResources + ' records of resource ' + orderedResource.resource);
      this.count++
      if (!data.resource || !data.resource.resourceType) {
        return nxtResource()
      }
      let id = data.resource.resourceType + '/' + data.resource.id;
      let processed = processedRecords.find((record) => {
        return record === id
      })
      if (!processed) {
        processedRecords.push(id)
      } else {
        return nxtResource()
      }
      let deleteRecord = false;
      let queries = [];
      // just in case there are multiple queries
      if (orderedResource.query) {
        queries = orderedResource.query.split('&');
      }
      for (let query of queries) {
        let limits = query.split('=');
        let limitParameters = limits[0];
        let limitValue = this.dataTypeConversion(limits[1]);
        if (!limitValue && limitValue !== false) {
          limitValue = ''
        }
        let resourceValue
        try {
          resourceValue = fhirpath.evaluate(data.resource, limitParameters);
        } catch (error) {
          logger.error(error);
        }
        if (Array.isArray(resourceValue) && !resourceValue.includes(limitValue)) {
          //delete this entry as it is no longer meet filters
          deleteRecord = true
        } else if (limitValue && !resourceValue) {
          //delete this entry as it is no longer meet filters
          deleteRecord = true
        } else if (resourceValue.toString() != limitValue.toString()) {
          //delete this entry as it is no longer meet filters
          deleteRecord = true
        }
      }
      let record = {};
      (async () => {
        if(orderedResource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
          for (let element of orderedResource["http://ihris.org/fhir/StructureDefinition/iHRISReportElement"]) {
            let fieldLabel
            let fieldName
            let fieldAutogenerated = false
            for (let el of element) {
              let value = '';
              for (let key of Object.keys(el)) {
                if (key !== 'url') {
                  value = el[key];
                }
              }
              if (el.url === "label") {
                let fleldChars = value.split(' ')
                //if label has space then format it
                if (fleldChars.length > 1) {
                  fieldLabel = value.toLowerCase().split(' ').map(word => word.replace(word[0], word[0].toUpperCase())).join('');
                } else {
                  fieldLabel = value
                }
              } else if (el.url === "name") {
                fieldName = value
              } else if (el.url === "autoGenerated") {
                fieldAutogenerated = value
              }
            }
            let displayData
            try {
              displayData = fhirpath.evaluate(data.resource, fieldName);
            } catch (error) {
              logger.error(error);
            }
            let value
            if ((!displayData || (Array.isArray(displayData) && displayData.length === 1 && displayData[0] === undefined)) && data.resource.extension) {
              try {
                value = await this.getElementValFromExtension(data.resource.extension, fieldName)
              } catch (error) {
                logger.error(error);
              }
            } else if (Array.isArray(displayData) && displayData.length === 1 && displayData[0] === undefined) {
              value = undefined
            } else if (Array.isArray(displayData)) {
              if(fieldLabel.startsWith('__') && orderedResource.multiple) {
                // value = displayData
                value = displayData.pop();
              } else {
                value = displayData.pop();
              }
            } else {
              value = displayData;
            }
            if (value || value === 0 || value === false) {
              if (typeof value == 'object') {
                if (value.reference && fieldAutogenerated) {
                  value = value.reference
                } else if (value.reference && !fieldAutogenerated) {
                  let referencedResource
                  try {
                    referencedResource = await this.getResourceFromReference(value.reference);
                  } catch (error) {
                    logger.error(error);
                  }
                  if (referencedResource) {
                    value = referencedResource.name
                  }
                } else {
                  value = JSON.stringify(value)
                }
              }
              if (fieldName === 'id') {
                value = data.resource.resourceType + '/' + value
              }
              record[fieldLabel] = value
            } else {
              record[fieldLabel] = null
            }
          }
        }
        record[orderedResource.name] = id
        let match = {};
        if (orderedResource.hasOwnProperty('linkElement')) {
          let linkElement = orderedResource.linkElement.replace(orderedResource.resource + '.', '');
          let linkTo
          try {
            linkTo = fhirpath.evaluate(data.resource, linkElement);
          } catch (error) {
            logger.error(error);
          }
          if (linkElement === 'id') {
            linkTo = orderedResource.resource + '/' + linkTo
          }
          if(!Array.isArray(linkTo)) {
            if(!linkTo) {
              linkTo = []
            } else {
              linkTo = [linkTo]
            }
          }
          match['__' + orderedResource.name + '_link' + '.keyword'] = linkTo;
        } else {
          match[orderedResource.name + '.keyword'] = [data.resource.resourceType + '/' + data.resource.id];
        }
        let ctx = '';
        for (let field in record) {
          // cleaning to escape ' char
          if (record[field] && typeof record[field] === 'string') {
            let recordFieldArr = record[field].split('')
            for (let recordFieldIndex in recordFieldArr) {
              let char = recordFieldArr[recordFieldIndex]
              if (char === "'") {
                recordFieldArr[recordFieldIndex] = "\\'"
              }
            }
            record[field] = recordFieldArr.join('');
          }
          if(deleteRecord || !record[field]) {
            ctx += 'ctx._source.' + field + "=null;";
            if(field.startsWith('__')) {
              let truncateResources = this.getChildrenResources(orderedResource.name)
              let truncateFields = this.getResourcesFields(truncateResources)
              for(let truncField of truncateFields) {
                ctx += 'ctx._source.' + truncField.field + "=null;";
              }
            }
          } else {
            ctx += 'ctx._source.' + field + "='" + record[field] + "';";
          }
        }
        // truncate fields of any other resources that are linked to this resource
        if(deleteRecord) {
          let childrenResources = this.getChildrenResources(orderedResource.name);
          let fields = this.getResourcesFields(childrenResources)
          for(let field of fields) {
            ctx += 'ctx._source.' + field.field + "=null;";
          }
        }

        let body = {
          script: {
            lang: 'painless',
            source: ctx
          },
          query: {
            terms: match ,
          },
        };
        if(!deleteRecord) {
          this.updateESDocument(body, record, reportDetails.name, orderedResource, data.resource, deleteRecord, () => {
            return nxtResource();
          })
        } else {
          //if this is the primary resource then delete the whole document, otherwise delete respective fields data
          if(!orderedResource.hasOwnProperty('linkElement')) {
            try {
              await this.deleteESDocument({query: body.query}, reportDetails.name)
            } catch (error) {
              logger.error(error);
            }
            return nxtResource();
          } else {
            this.updateESDocument(body, record, reportDetails.name, orderedResource, data.resource, deleteRecord, () => {
              return nxtResource();
            })
          }
        }
      })();
    }, () => {
      return callback()
    });
  }

  fixDataInconsistency(reportDetails, orderedResource, callback) {
    logger.info("Fixing data inconsistency for resource " + orderedResource.resource)
    //these must be run in series
    let fieldStillMissing = false
    async.series({
      //this fix missing data i.e __location_link is available but location is missing
      fixMissing: (callback) => {
        let query = {
          query: {
            bool: {
              must_not: {
                exists: {
                  field: orderedResource.name
                }
              }
            }
          }
        }
        runCleaner(query, false, this, (missing) => {
          if(missing) {
            fieldStillMissing = true
          }
          return callback(null)
        })
      },
      //this fix invalid data i.e __location_link not equal to location, and location is always invalid, not __location_link
      differences: (callback) => {
        if(!orderedResource.linkElement || fieldStillMissing) {
          return callback(null)
        }
        let query = {
          query: {
            bool: {
              must: {
                script: {
                  script: {
                    source: `if(doc['__${orderedResource.name}_link.keyword'].size() != 0 && doc['${orderedResource.name}.keyword'].size() != 0) {if(doc['__${orderedResource.name}_link.keyword'].value != doc['${orderedResource.name}.keyword'].value){return true}}`,
                    lang: "painless"
                  }
                }
              }
            }
          }
        }
        runCleaner(query, true, this, () => {
          return callback(null)
        })
      }
    }, () => {
      return callback()
    })

    function runCleaner(query, ignoreReverseLinked, me, callback) {
      try {
        me.getESDocument(reportDetails.name, query, (err, documents) => {
          if(documents.length === 0) {
            return callback(null)
          }
          /**
           * this is for reversed linked resources i.e Practitioner and PractitionerRole
           */
          let reverseLink = false
          // end of reversed linked resources
          let resIds = []
          async.eachOf(documents, (doc, index, nxtDoc) => {
            if(doc._source['__' + orderedResource.name + '_link']) {
              if(resIds.length === 0) {
                let linkResType = doc._source['__' + orderedResource.name + '_link'].split('/')[0]
                if(linkResType !== orderedResource.resource) {
                  reverseLink = true
                  if(ignoreReverseLinked) {
                    return nxtDoc();
                  }
                }
              }
              resIds.push(doc._source['__' + orderedResource.name + '_link'])
            } else {
              //logger.error(JSON.stringify(doc,0,2));
              //logger.error('There is a serious data inconsistency that needs to be addressed on index ' + reportDetails.name + ' and index id ' + doc._id + ', field ' + '__' + orderedResource.name + '_link' + ' is missing');
              return nxtDoc();
            }
            let ids
            if(resIds.length === 100 || index === (documents.length - 1)) {
              ids = resIds.join(',')
              resIds = []
            } else {
              return nxtDoc()
            }

            if(!ids || ids == 'null' || (ignoreReverseLinked && reverseLink)) {
              return nxtDoc()
            }
            let processedRecords = []
            let url = URI(me.FHIRBaseURL)
              .segment(orderedResource.resource)
              .addQuery('_count', 200)
            if(!reverseLink) {
              url = url.addQuery('_id', ids)
            } else {
              if(!orderedResource.linkElementSearchParameter) {
                logger.error('linkElementSearchParameter is missing, cant fix data inconsistency');
                return callback()
              }
              url = url.addQuery(orderedResource.linkElementSearchParameter, ids)
            }
            url = url.toString()
            async.whilst(
              (callback) => {
                return callback(null, url !== null)
              },
              (callback) => {
                axios.get(url, {
                  withCredentials: true,
                  auth: {
                    username: me.FHIRUsername,
                    password: me.FHIRPassword,
                  },
                }).then(response => {
                  me.totalResources = response.data.total;
                  url = null;
                  const next = response.data.link.find(
                    link => link.relation === 'next'
                  );
                  if (next) {
                    url = next.url
                  }
                  if (response.data.total > 0 && response.data.entry && response.data.entry.length > 0) {
                    me.count = 1;
                    me.processResource(response.data.entry, orderedResource, reportDetails, processedRecords, () => {
                      return callback(null, url);
                    })
                  } else {
                    return callback(null, url);
                  }
                }).catch((err) => {
                  logger.error('Error occured while getting resource data');
                  logger.error(err);
                  return callback(null, null)
                })
              },
              () => {
                return nxtDoc()
              }
            )
          }, async() => {
            try {
              await me.refreshIndex(reportDetails.name);
            } catch (error) {
              logger.error(error);
            }
            return callback()
          })
        })
      } catch (error) {
        logger.error(error);
        return callback()
      }
    }
  }

  dataTypeConversion(value) {
    var v = Number (value);
    return !isNaN(v) ? v :
         value === "undefined" ? undefined
       : value === "null" ? null
       : value === "true" ? true
       : value === "false" ? false
       : value
    }
}
module.exports = {
  CacheFhirToES
}
