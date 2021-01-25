const express = require("express");
const app = express();

// Constants
const WORK_QUEUE = "WORK_QUEUE";
const BACKUP_QUEUE = "BACKUP_QUEUE";
const REDIS_PORT = 6379;
const INDEX_NAME = "adbi";

var Redis = require('ioredis');
var redisClient1 = new Redis();
var Redis = require('redis');
const redisClient = Redis.createClient(REDIS_PORT);

const { promisify } = require("util");
const hgetallAsync = promisify(redisClient.hgetall).bind(redisClient);
const typeAsync = promisify(redisClient.type).bind(redisClient);
const smembersAsync = promisify(redisClient.smembers).bind(redisClient);

var elasticsearch = require("elasticsearch");
// Elastic Search
var elasticClient = new elasticsearch.Client({
  host: "http://localhost:9200",
  log: "info",
});

var redisLoop = function () {
  redisClient1.brpoplpush(WORK_QUEUE, BACKUP_QUEUE, 0).then(function (plan_id) {
      // because we are using BRPOPLPUSH, the client promise will not resolve
      // until a 'result' becomes available
      processJob(plan_id);
      // delete the item from the working channel, and check for another item
      redisClient1.lrem(BACKUP_QUEUE, 1, plan_id).then(redisLoop);
  });
};

redisLoop();

async function processJob(planId) {
  let id = planId.split("_")[1];

  if(planId.includes('delete')){
    deletePlan(id);
  }else{
    let resJSON = {};
    await recreateJSON(planId, resJSON); 
    indexPlan(id, resJSON);
  }
}

async function indexPlan(id, plan){
  console.log("Starting to index plan id ==> " + id);
  plan['plan_service'] = { name: "plan" };

  let planCostShares = plan['planCostShares'];
  let linkedPlanServices = plan['linkedPlanServices'];

  delete plan['planCostShares'];
  delete plan['linkedPlanServices'];

  await indexPartialPlan(id, plan);

  planCostShares['plan_service'] = {
    name: "membercostshare",
    parent: plan.objectId
  };

  await indexPartialPlan(planCostShares.objectId, planCostShares);

  for(let i =0; i < linkedPlanServices.length; i++){
    let planservice = linkedPlanServices[i];

    let linkedService = planservice['linkedService'];
    let planserviceCostShares = planservice['planserviceCostShares'];
  
    delete planservice['linkedService'];
    delete planservice['planserviceCostShares'];

    planservice['plan_service'] = {
      name: "planservice",
      parent: plan.objectId
    };

    await indexPartialPlan(planservice.objectId, planservice);

    linkedService['plan_service'] = {
      name: "service",
      parent: planservice.objectId
    };

    await indexPartialPlan(linkedService.objectId, linkedService);


    planserviceCostShares['plan_service'] = {
      name: "planservice_membercostshare",
      parent: planservice.objectId
    };

    await indexPartialPlan(planserviceCostShares.objectId, planserviceCostShares);
  } 

  console.log("Indexed successfully");
}

async function indexPartialPlan(id, body){
  elasticClient.index({
    index: INDEX_NAME,
    type: '_doc',
    id: id,
    body: body,
    routing: "1"
  });
}

function deletePlan(id){
  console.log("Starting to delete plan id ==> " + id);
  elasticClient.delete({
    id: id,
    index: INDEX_NAME,
    type: "plan",
  }, function(error, response){
    if(error == undefined){
      console.log("Deleted successfully");
    }else{
      console.log("Deletion failed");
      console.log("error", error);
    }
  });
}

async function recreateJSON(id, resJSON) {
  await typeAsync(id).then(async (res) => {
    if (res === "hash") {
      await hgetallAsync(id).then(async (response) => {
        for(key in response){
           let value = response[key];
           let currentKey = key;
           await typeAsync(value).then(async (res) => {
              if(res === "hash"){
                newHashJSON = {};
                resJSON[currentKey] = newHashJSON;           
                await recreateJSON(value, newHashJSON);
              }else if(res === "set"){
                newArr = [];
                resJSON[currentKey] = newArr;
                await recreateJSON(value, newArr);             
              }else{
                // when type is null it means it has a simple property
                resJSON[currentKey] = isNaN(value) ? value: parseInt(value);
              }
           });
        }
      });
    } else if (res === "set") {
      await smembersAsync(id).then(async (result) => {
        for(let i=0;i<result.length;i++){
          await typeAsync(result[i]).then(async (res) => {
            if(res === "hash"){
              newHashJSON = {};
              resJSON.push(newHashJSON);                           
              await recreateJSON(result[i], newHashJSON);                 
            }else if(res === "set"){
              newArr = [];
              resJSON.push(newArr)            
              await recreateJSON(result[i], newArr);             
            }else{
              // when type is null it means it has a simple property
              resJSON.push(result[i]);
            }
          })
        }
      });
    } 
  });
  return resJSON;
}

module.exports = app;
