const express = require('express')
const app = express()
const { MeiliSearch } = require('meilisearch')
const cors = require('cors');
const bodyParser = require('body-parser')
const { io } = require("socket.io-client");
require('dotenv').config()

app.use (bodyParser.json ({limit: '10mb', extended: true}))
app.use (bodyParser.urlencoded ({limit: '10mb', extended: true}))
const corsOptions ={
    origin:'http://localhost:5173', 
    credentials:true,         
    optionSuccessStatus:200
}
app.use(cors(corsOptions))

const client = new MeiliSearch({
  host: process.env.HOST,
  apiKey: process.env.API_KEY
})

const socket = io("https://api-dev.streammix.co");
const ISEMPTY = 'isEmpty'
socket.on("connect", () => {
  })

function connectSocketStreammix (idChannel) {
  try{
    console.log(idChannel)
      socket.emit("call", "live.subscribe", { id: idChannel }, function (err, res) {
        if (err) {
          console.log(err, "error");
        } else {
          console.log("call success in controller:", res);
        }
      });
      socket.on("comment", (cm) => {
        handleCommentsData(cm, idChannel)
      });
      socket.on("share", (cm) => {
        addShareIndexes(cm, idChannel)
        
      });
      socket.on("reaction", (cm) => {
        console.log(cm)
        addReactionIndexes(cm, idChannel)
      });
  } catch(e) {
    console.log(e)
  }
}
  

const convertCommentToObj = (comment, isFirstComment) => {
  return {
    id: comment.id.replace("comment:", ""),
    text: comment.text.replace('"', ''),
    author_id: comment.author.id,
    author_name: comment.author.name,
    author_avatar: comment.author.avatar,
    metadata_url: comment.metadata.url,
    platform_icon: comment.platform.icon,
    platform_name: comment.platform.name,
    outId: comment.outId,
    serverTime: comment.serverTime,
    workerId: comment.workerId,
    timestamp: comment.timestamp,
    timeConvert: comment.timeConvert,
    source: comment.source,
    isFirstComment
  }
}
const handleCommentsData = async (comments, idChannel) => {
  let commentsArrayFlat = []
  let promises = []
  comments.forEach((comment) => {
    const processCheckComment = client.index('comments_' + idChannel).getDocuments({
      filter: `text = "${comment.text.replace('"', '')}" AND author_id = '${comment.author.id}'`,
      fields: ['text'],
      limit: 1
    })
    .then(commentExit => {
      if (commentExit.results.length > 0) {
        commentsArrayFlat.push(convertCommentToObj(comment, isFirstComment = false))
      } else {
        commentsArrayFlat.push(convertCommentToObj(comment, isFirstComment = true))
      }
    })
    .catch(() => {
      commentsArrayFlat.push(convertCommentToObj(comment, isFirstComment = true))
    })
    promises.push(processCheckComment);
  });

  Promise.all(promises)
  .then(() => {
    addCommentIndexes(commentsArrayFlat, idChannel)
  })
}

const checkDuplicateCommentInMoment = (comments) => {
  const newComment = []
  comments.forEach((comment) => {
    if(newComment.some(c => c.text == comment.text && c.author_id == comment.author_id)) {
      comment.isFirstComment = false
      newComment.push(comment)
    } else {
      newComment.push(comment)
    }
  })
  return newComment
}

const handleReactionsData = (reactions) => {
  let newReactions = []
  reactions.forEach(reaction => {
    newReactions.push({
      id: reaction.author.id
    })
  })
  return newReactions
}

const handleShareData = (shares) => {
  let newShares= []
  shares.forEach(share => {
    newShares.push({
      id: share.id
    })
  })
  return newShares
}

const addCommentIndexes = async (comments, idChannel) => {
  if(comments?.length > 0) {
    const commentsArrayFlat = checkDuplicateCommentInMoment(comments)
    client.index('comments_' + idChannel).addDocuments(commentsArrayFlat,  { primaryKey: 'id' })
  }
}

const addReactionIndexes = (reaction, idChannel) => {
  const reactions = handleReactionsData(reaction)
  client.index('reactions_' + idChannel).addDocuments(reactions)
}

const addShareIndexes = (share, idChannel) => {
  const shares = handleShareData(share)
  client.index('share_' + idChannel).addDocuments(shares)
}

const getFilterString = async (filter, idChannel, searchQuery, valueFilter) => {
  switch(filter) {
    case 'reacted':
      try {
        const reactions = await client.index('reactions_' + idChannel).getDocuments({limit: 50000})
        if(reactions) return `(author_id IN [${reactions.results.map(reaction => reaction.id)}])`
      } catch {
        return ISEMPTY
      }
    case 'shared':
      try {
        const shares = await client.index('share_' + idChannel).getDocuments({limit: 1000})
        if(shares) return `(author_id IN [${shares.results.map(share => share.id)}])`
      } catch {
        return ISEMPTY
      }
    case 'perfectMatch':
      return `(text = '${searchQuery}')`
    case 'filterByTime':
      if(valueFilter[0] > 0 && valueFilter[1] > 0) return `(timestamp ${valueFilter[0]} TO ${valueFilter[1]})`
      else return ''
    case 'removeDuplicateComment':
      return `(isFirstComment = true)`
    case 'filterLatestComment':
      if(valueFilter.length > 0) return `(isFirstComment = true) AND (text = '${searchQuery}')`
      else return ''
    default:
      return ''
  }
}

const handleFilter = async (filter, idChannel, searchQuery) => {
  let stringFilter = ''
  for (let key in filter) {
    if(filter[key]) {
      const str = await getFilterString(key, idChannel, searchQuery, filter[key])
      if(str == ISEMPTY){
        stringFilter = ISEMPTY
        break
      } 
      if(stringFilter && str) stringFilter += (` AND ` + str)
      else stringFilter += str 
    }
  }
  return stringFilter
}


const checkInvalidQuery = (filter, searchQuery) => {
  if((filter.filterLatestComment?.length || filter.caseSensitive || filter.perfectMatch) && !searchQuery) {
    return true
  } else return false
}

const checkExitsLastComment = (lastComment, searchQuery) => {
  if(lastComment.length === 0) return true
  else if(lastComment.some(comment => comment.toLowerCase() == searchQuery.toLowerCase())) {
    return true
  } else return false
}

const getCaseSensitive = (comments, searchQuery) => {
  return comments.map(comment => {
    if(comment.text.includes(searchQuery)) return comment
  }).filter(comment => comment !== undefined)
}

app.post('/createIndexesMeiliSearch/:idChannel', async (req, res) => {
  try {
    const processUpdateFilterAble = client.index('comments_' + req.params.idChannel).updateFilterableAttributes([
      'author_id',
      'text',
      'timestamp',
      'isFirstComment'
    ])
    const processUpdateSearchAble = client.index('comments_' + req.params.idChannel).updateSearchableAttributes([
      'text',
    ])
    client.createIndex('reactions_' + req.params.idChannel)
    client.createIndex('share_' + req.params.idChannel)
    client.createIndex('comments_' + req.params.idChannel, { primaryKey: 'id' })
    .then(() => {
      return Promise.all([
        processUpdateFilterAble,
        processUpdateSearchAble,
        client.index('comments_' + req.params.idChannel).updateSettings({ pagination: { maxTotalHits: 1000000 }}),
        client.index('reaction_' + req.params.idChannel).updateSettings({ pagination: { maxTotalHits: 50000 }})
      ])
    })
    .then(() => {
      connectSocketStreammix(req.params.idChannel)
      res.json({success: true, message: 'create indexes success'})
    })
  } catch {
    res.status(500).json({success: false, message: 'invalid create indexes'})
  }
   
})


app.delete('/deleteIndexesMeilisearch/:idChannel', (req, res) => {
  Promise.all([
    client.deleteIndex('comments_' + req.params.idChannel),
    client.deleteIndex('reactions_' + req.params.idChannel),
    client.deleteIndex('share_' + req.params.idChannel)
  ])
  .then(() => {
    res.status(200).json({success: true, message: 'delete indexes meilisearch success'})
  })
  .catch(e => {
    res.status(500).json({success: false, message: 'invali remove indexes meilisearch ' + e})
  })
})

app.post('/meiliSearchFilter/:idChannel', async (req, res) => {
  try {
    let searchStr = req.query.search ? req.query.search : ''
    let limit = req.body.rowsPaging ? parseInt(req.body.rowsPaging) : 20
    let offset = req.body.pagePaging > 1 ? (req.body.pagePaging - 1) * limit : 0
    let filterByCaseSensitive = req.body.caseSensitive
    let lastComment = req.body.filterLatestComment
    if(checkInvalidQuery(req.body, searchStr)) {
      res.json({success: false, message: 'Invalid filter'})
    } else {
      const stringFilter = await handleFilter(req.body, req.params.idChannel, searchStr)
    
      if(checkExitsLastComment(lastComment, searchStr) && (stringFilter !== ISEMPTY)) {
        client.index('comments_' + req.params.idChannel).search(searchStr, {
          filter: stringFilter,
          limit,
          offset,
          matchingStrategy: 'all'
        })
        .then(comments => {
          if(filterByCaseSensitive) comments.hits = getCaseSensitive(comments.hits, searchStr)
          res.status(200).json({comments})
        })
      } else {
        res.status(200).json({success: true, comments: {hits:[]}})
      }
    }
  } catch(e) {
    res.status(500).json({success: false, message: 'invali server when search'})
  }
  
})

app.listen(3000, () => {
  console.log('port ' + process.env.PORT)
})
