package redisReplicaManager

import (
	redisLuaScriptUtils "github.com/zavitax/redis-lua-script-utils-go"
)

var scriptAddSlotSite = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySlotSitesRolesHash", "keyPubsubChannel"},
	[]string{"argSiteID", "argSlotID"},
	`
		local existingReplicaCount = tonumber(redis.call('HLEN', keySlotSitesRolesHash));
		
		local existingSiteRole = redis.call('HGET', keySlotSitesRolesHash, argSiteID)
		
		-- Check if there are any masters
		local newSiteRole = 'master';
		
		if existingSiteRole ~= 'master' then
			local sitesRolesValues = redis.call('HVALS', keySlotSitesRolesHash);
		
			for i, slotRole in ipairs(sitesRolesValues) do
				if slotRole == 'master' then
					newSiteRole = 'normal';
					break;
				end
			end

			redis.call('HSET', keySlotSitesRolesHash, argSiteID, newSiteRole);
		end
		
		local newReplicaCount = tonumber(redis.call('HLEN', keySlotSitesRolesHash));
		
		--redis.call('HSET', keySlotsReplicaCountHash, argSlotID, newReplicaCount);
		
		local added = 0;
		if existingReplicaCount ~= newReplicaCount then
			added = 1;
		
			-- Announce site added
			redis.call('PUBLISH', keyPubsubChannel, cjson.encode({
						event = 'slot_site_added',
						slot = argSlotID,
						site = argSiteID,
						role = newSiteRole
			}));
		
			if newSiteRole == 'master' then
				-- Announce slot master changed
				redis.call('PUBLISH', keyPubsubChannel, cjson.encode({
							event = 'slot_master_change',
							slot = argSlotID,
							site = argSiteID,
							reason = 'slot_site_added'
				}));
			end
		end
		
		return { added, newSiteRole, newReplicaCount };
	`)

var scriptConditionalRemoveSlotSite = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySlotSitesRolesHash", "keyPubsubChannel"},
	[]string{"argSiteID", "argSlotID", "argMinReplicaCount", "argReason"},
	`
		local existingReplicaCount = tonumber(redis.call('HLEN', keySlotSitesRolesHash))
		
		if existingReplicaCount <= tonumber(argMinReplicaCount) then
			-- Minimum replica count not satisfied
			return { 0, existingReplicaCount, nil };
		end
		
		-- Get removed site former Role
		local removedSiteRole = redis.call('HGET', keySlotSitesRolesHash, argSiteID)

		-- Remove site & get amount of removed sites
		local removedSitesCount = redis.call('HDEL', keySlotSitesRolesHash, argSiteID);
		
		-- Get new replica count after site was removed
		local newReplicaCount = tonumber(redis.call('HLEN', keySlotSitesRolesHash));

		-- Get remaining sites Roles to figure out if a new master is required
		local remainingSites = redis.call('HKEYS', keySlotSitesRolesHash)
		
		if remainingSites == nil then
			remainingSites = {};
		end
		
		if removedSitesCount > 0 then
			-- Announce that site was removed
			redis.call('PUBLISH', keyPubsubChannel, cjson.encode({
						event = 'slot_site_removed',
						slot = argSlotID,
						site = argSiteID,
						reason = argReason,
						role = removedSiteRole
			}));
		end
		
		if removedSiteRole == 'master' and #remainingSites > 0 then
			-- The site we removed was the master for this slot, select a new master
			local newMasterSiteID = remainingSites[1]
		
			redis.call('HSET', keySlotSitesRolesHash, newMasterSiteID, 'master');

			-- Announce slot master changed
			redis.call('PUBLISH', keyPubsubChannel, cjson.encode({
						event = 'slot_master_change',
						slot = argSlotID,
						site = newMasterSiteID,
						reason = 'slot_site_removed'
			}));

			return { removedSitesCount, newReplicaCount, newMasterSiteID }
		else
			return { removedSitesCount, newReplicaCount, nil }
		end
	`)

var scriptUpdateSiteSlotChangeSnippet = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySlotSitesRolesHash", "keySiteSlotsHash"},
	[]string{"argSiteID", "argSlotID"},
	`
		local function parse_json(input, defaultValue)
			local success, result = pcall(function(input) return cjson.decode(input) end, input);
			
			if success then
				return result;
			else
				return defaultValue;
			end
		end

		local currentSiteRoleInSlot = redis.call('HGET', keySlotSitesRolesHash, argSiteID)

		-- Get current slots per site
		local existingSiteSlotsTable = parse_json(redis.call('HGET', keySiteSlotsHash, argSiteID), {});
		local newSiteSlotsTable = {};

		for k, v in pairs(existingSiteSlotsTable) do
			if k == argSlotID then
				-- Current argSlotID, check for existence
				if currentSiteRoleInSlot ~= nil then
					-- Site exists in slot
					newSiteSlotsTable[k] = v;
				end
			else
				-- Other slot IDs
				newSiteSlotsTable[k] = v;
			end
		end

		if currentSiteRoleInSlot ~= nil then
			-- Add new site if not exists
			newSiteSlotsTable[argSlotID] = currentSiteRoleInSlot;
		end

		redis.call('HSET', keySiteSlotsHash, argSiteID, cjson.encode(newSiteSlotsTable))

		return nil;
	`)

var scriptUpdateSiteTimestampSnippet = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySitesTimestamps"},
	[]string{"argSiteID", "argCurrentTimestamp"},
	`
		return tonumber(redis.call('ZADD', keySitesTimestamps, 'GT', tonumber(argCurrentTimestamp), argSiteID));
	`)

var scriptGetTimedOutSites = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySitesTimestamps", "keySiteSlotsHash"},
	[]string{"argOldestTimestamp"},
	`
		local function parse_json(input, defaultValue)
			local success, result = pcall(function(input) return cjson.decode(input) end, input);
			
			if success then
				return result;
			else
				return defaultValue;
			end
		end

		local sites = redis.call('ZRANGEBYSCORE', keySitesTimestamps, '-inf', tonumber(argOldestTimestamp));

		if sites == nil then
			return {}
		end

		local result = {}

		for siteIndex, siteID in ipairs(sites) do
			local siteSlotsArray = {}
			local siteSlotsTable = parse_json(redis.call('HGET', keySiteSlotsHash, siteID), {});
			
			for slotID, slotRole in pairs(siteSlotsTable) do
				siteSlotsArray[#siteSlotsArray + 1] = slotID
			end

			result[siteIndex] = { siteID, siteSlotsArray }
		end

		return result
	`)

var scriptGetSiteSlotInfo = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySlotSitesRolesHash", "keySiteSlotsHash"},
	[]string{"argSiteID", "argSlotID"},
	`
		local role = redis.call('HGET', keySlotSitesRolesHash, argSiteID)

		if role == nil then
			return {}
		else
			return { role }
		end
	`)

var scriptGetSiteSlots = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySiteSlotsHash"},
	[]string{"argSiteID"},
	`
		local function parse_json(input, defaultValue)
			local success, result = pcall(function(input) return cjson.decode(input) end, input);
			
			if success then
				return result;
			else
				return defaultValue;
			end
		end

		-- Get current slots per site
		local existingSiteSlotsTable = parse_json(redis.call('HGET', keySiteSlotsHash, argSiteID), {});
		local result = {};

		for k, v in pairs(existingSiteSlotsTable) do
			result[#result + 1] = { k, v }
		end

		return result;
	`)

var scriptGetAllSiteIDs = redisLuaScriptUtils.NewRedisScript(
	[]string{"keySitesTimestamps"},
	[]string{},
	`
		local result = redis.call('ZRANGEBYSCORE', keySitesTimestamps, '-inf', '+inf');

		if result == nil then
			result = {};
		end

		return result;
	`)
