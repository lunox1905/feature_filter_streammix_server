const express = require('express')
const app = express()
const { MeiliSearch } = require('meilisearch')
const comments = require('./api.json')
const cors = require('cors');
const bodyParser = require('body-parser')
require('dotenv').config()

app.use (bodyParser.json ({limit: '10mb', extended: true}))
app.use (bodyParser.urlencoded ({limit: '10mb', extended: true}))
const corsOptions ={
    origin:'http://localhost:5173', 
    credentials:true,            //access-control-allow-credentials:true
    optionSuccessStatus:200
}
app.use(cors(corsOptions))

const client = new MeiliSearch({
  host: process.env.HOST,
  apiKey: process.env.API_KEY
})

const commentsArray = Object.values(comments.comment)

const handleCommentsData = (comments) => {
  let commentsArrayFlat = []
  comments.forEach((comment) => {
    if(commentsArrayFlat.some(c => c.text == comment.text && c.author_id == comment.author.id)) {
      commentsArrayFlat.push({
        id: comment.id.replace("comment:", ""),
        text: comment.text,
        author_id: comment.author.id,
        author_name: comment.author.name,
        author_avatar: comment.author.name,
        metadata_url: comment.metadata.url,
        platform_icon: comment.platform.icon,
        platform_name: comment.platform.name,
        outId: comment.outId,
        serverTime: comment.serverTime,
        workerId: comment.workerId,
        timestamp: comment.timestamp,
        timeConvert: comment.timeConvert,
        source: comment.source,
        isFirstComment: false
      })
    } else {
      commentsArrayFlat.push({
        id: comment.id.replace("comment:", ""),
        text: comment.text,
        author_id: comment.author.id,
        author_name: comment.author.name,
        author_avatar: comment.author.name,
        metadata_url: comment.metadata.url,
        platform_icon: comment.platform.icon,
        platform_name: comment.platform.name,
        outId: comment.outId,
        serverTime: comment.serverTime,
        workerId: comment.workerId,
        timestamp: comment.timestamp,
        timeConvert: comment.timeConvert,
        source: comment.source,
        isFirstComment: true
      })
    }
  });
  return commentsArrayFlat
}

const handleReactionsData = (reactions) => {
  let newReactions = []
  Object.keys(reactions).forEach(reaction => {
    newReactions.push({
      id: reaction
    })
  })
  return newReactions
}

const handleShareData = (shares) => {
  let newShares= []
  Object.keys(shares).forEach(share => {
    newShares.push({
      id: share
    })
  })
  return newShares
}

const getFilterString = async (filter, idChannel, searchQuery, valueFilter) => {
  switch(filter) {
    case 'reacted':
      const reactions = await client.index('reactions_' + idChannel).getDocuments()
      return `(author_id IN [${reactions.results.map(reaction => reaction.id)}])`
    case 'shared':
      const shares = await client.index('share_' + idChannel).getDocuments()
      return `(author_id IN [${shares.results.map(share => share.id)}])`
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
      if(stringFilter && str) stringFilter += (` AND ` + str)
      else stringFilter += str 
    }
  }
  return stringFilter
}


const checkInvalidQuery = (filter, searchQuery) => {
  if((filter.filterLatestComment.length || filter.caseSensitive || filter.perfectMatch) && !searchQuery) {
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


app.get('/createIndexesMeiliSearch/:idChannel', async (req, res) => {
  try {
    const reactions = handleReactionsData(comments.reaction)
    const commentsArrayFlat = handleCommentsData(commentsArray)
    const shares = handleShareData(comments.share)
    const idChannel = req.params.idChannel

    const processUpdateFilterAble = client.index('comments_' + idChannel).updateFilterableAttributes([
      'author_id',
      'text',
      'timestamp',
      'isFirstComment'
    ])
    const processUpdateSearchAble = client.index('comments_' + idChannel).updateSearchableAttributes([
      'text',
    ])
    if(shares) client.index('share_' + idChannel).addDocuments(shares)
    client.index('reactions_' + idChannel).addDocuments(reactions)
    client.index('comments_' + idChannel).addDocuments(commentsArrayFlat, {primaryKey: 'id'})
    .then(() => {
      return Promise.all([processUpdateFilterAble, processUpdateSearchAble])
    })
    .then(() => {
      res.status(200).json({success: true, message: 'Create indexes success'})
    })
  } catch {
    res.status(500).json({success: false, message: 'invalid create indexes'})
  }
   
})


app.put('/updateReactionIndexMeiliSearch/:idChannel', (req, res) => {
  client.index('reactions_' + req.params.idChannel).addDocuments(req.body.reactions)
  .then(() => {
    res.status(200).json({success: true, message: 'update reaction index succses'})
  })
  .catch(e => {
    res.status(500).json({success: false, message: e})
  })
})

app.put('/updateShareIndexMeiliSearch/:idChannel', (req, res) => {
  client.index('share_' + req.params.idChannel).addDocuments(req.body.share)
  .then(() => {
    res.status(200).json({success: true, message: 'update share index succses'})
  })
  .catch(e => {
    res.status(500).json({success: false, message: e})
  })
})


app.put('/updateCommentsIndexMeiliSearch/:idChannel', (req, res) => {
  const comments = req.body
  const commentsArrayFlat = []
  const promises = [];

  comments.forEach(comment => {
    const promise = new Promise(async (resolve, reject) => {
      try {
        const isExitsComment = await client.index('comments_' + req.params.idChannel).getDocuments({
          filter: `text = '${comment.text}' AND author_id = '${comment.author.id}'`,
          fields: ['text'],
          limit: 1
        });

        if (isExitsComment.results) {
          commentsArrayFlat.push({
            id: comment.id.replace("comment:", ""),
            text: comment.text,
            author_id: comment.author.id,
            author_name: comment.author.name,
            author_avatar: comment.author.name,
            metadata_url: comment.metadata.url,
            platform_icon: comment.platform.icon,
            platform_name: comment.platform.name,
            outId: comment.outId,
            serverTime: comment.serverTime,
            workerId: comment.workerId,
            timestamp: comment.timestamp,
            timeConvert: comment.timeConvert,
            source: comment.source,
            isFirstComment: false
          });
        } else {
          commentsArrayFlat.push({
            id: comment.id.replace("comment:", ""),
            text: comment.text,
            author_id: comment.author.id,
            author_name: comment.author.name,
            author_avatar: comment.author.name,
            metadata_url: comment.metadata.url,
            platform_icon: comment.platform.icon,
            platform_name: comment.platform.name,
            outId: comment.outId,
            serverTime: comment.serverTime,
            workerId: comment.workerId,
            timestamp: comment.timestamp,
            timeConvert: comment.timeConvert,
            source: comment.source,
            isFirstComment: true
          });
        }

        resolve();
      } catch (error) {
        reject(error);
      }
    });

    promises.push(promise);
  });

  Promise.all(promises)
  .then(() => {
    client.index('comments_' + req.params.idChannel).addDocuments(commentsArrayFlat)
      .then(() => {
        res.status(200).json({ success: true, message: 'update comments index success' });
      })
      .catch(e => {
        res.status(500).json({ success: false, message: e });
      });
  })
  .catch(error => {
    res.status(500).json({ success: false, message: error });
  });
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
      if(checkExitsLastComment(lastComment, searchStr)) {
        const stringFilter = await handleFilter(req.body, req.params.idChannel, searchStr)
        client.index('comments_' + req.params.idChannel).search(searchStr, {
          filter: stringFilter,
          limit,
          offset
        })
        .then(comments => {
          if(filterByCaseSensitive) comments.hits = getCaseSensitive(comments.hits, searchStr)
          res.status(200).json({comments})
        })
      } else {
        res.json({success: true, comments: []})
      }
    }
  } catch(e) {
    console.log(e)
    res.status(500).json({success: false, message: 'invali server when search'})
  }
  
})
app.listen(3000, () => {
  console.log('port ' + process.env.PORT)
})