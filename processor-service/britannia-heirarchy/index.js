
var processor = new Processor({
  // name will be derived from package.json or the dir name
  channel: 'collector:britannia-heirarchy:view' // listen to this channel collector:collectorName:workitemType
})


// data will be coming from known collector service only -> (data.collector.signature in processor.collectorSignatures)
function acceptData(data) {
  // store the data using whatever data connectors - should return an object with data id
  // built-in data connector: cw-module-processor-datastore

  var message = data[data.length - 1] // the last one is the relevant one

  console.log(message.workitemType)
  console.log(message.lastSyncTime)
  console.log(message.origin) // which collector received this data
  console.log(message.data)

  // data store event will be automatically broadcasted -> processor:britannia-heirarchy:view
  // wi generator should listen on processor:britannia-heirarchy:view to create workitems out of it

  /*
  [
    {
      id: collector data id,
      workitemType: wi type,
      lastSyncTime: last sync time,
      origin: {
        type: collector,
        name: britannia-heirarchy,
        time: 789789789798
      }
      data: raw data
    },
    {
      id: processor data id,
      workitemType: wi type,
      lastSyncTime: last sync time,
      origin: {
        type: processor,
        name: britannia-heirarchy,
        time: 7878789789789
      }
      data: processed data
    }
  ]
  */
}
