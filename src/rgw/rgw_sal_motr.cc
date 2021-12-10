// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "motr/config.h"
#include "lib/types.h"
#include "lib/trace.h"   // m0_trace_set_mmapped_buffer
#include "motr/layout.h" // M0_OBJ_LAYOUT_ID

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_motr.h"
#include "rgw_bucket.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

  int MotrUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
      const string& end_marker, uint64_t max, bool need_stats,
      BucketList &buckets, optional_yield y)
  {
//    RGWUserBuckets ulist;
//    bool is_truncated = false;
//    int ret;
//
//    buckets.clear();
//    ret = store->getDB()->list_buckets(dpp, info.user_id, marker, end_marker, max,
//        need_stats, &ulist, &is_truncated);
//    if (ret < 0)
//      return ret;
//
//    buckets.set_truncated(is_truncated);
//    for (const auto& ent : ulist.get_buckets()) {
//      buckets.add(std::make_unique<MotrBucket>(this->store, ent.second, this));
//    }

    return 0;
  }

  Bucket* MotrUser::create_bucket(rgw_bucket& bucket,
      ceph::real_time creation_time)
  {
    return NULL;
  }

  int MotrUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
  {
//    int ret;
//    ret = store->getDB()->get_user(dpp, string("user_id"), "", info, &attrs,
//        &objv_tracker);
//    return ret;
    return 0;
  }

  int MotrUser::read_stats(const DoutPrefixProvider *dpp,
      optional_yield y, RGWStorageStats* stats,
      ceph::real_time *last_stats_sync,
      ceph::real_time *last_stats_update)
  {
    return 0;
  }

  /* stats - Not for first pass */
  int MotrUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB *cb)
  {
    return 0;
  }

  int MotrUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int MotrUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
      bool *is_truncated, RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int MotrUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int MotrUser::load_user(const DoutPrefixProvider *dpp, optional_yield y)
  {
//    int ret = store->getDB()->get_user(dpp, string("user_id"), "", info, &attrs,
//        &objv_tracker);
    int rc = store->open_idx(&idxID, true, &idx);
    if (rc != 0) {
      ldout(store->cctx, 0) << "ERROR: failed to open index: " << rc << dendl;
      goto out;
    }

  out:
    m0_idx_fini(&idx);
    return rc;
  }

  int MotrUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
  {
    //int ret = store->getDB()->store_user(dpp, info, exclusive, &attrs, &objv_tracker, old_info);

    return 0;
  }

  int MotrUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
  {
    //int ret = store->getDB()->remove_user(dpp, info, &objv_tracker);

    return 0;
  }

  int MotrBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y)
  {
    int ret;

    ret = get_bucket_info(dpp, y);
    if (ret < 0)
      return ret;

    /* XXX: handle delete_children */

    //ret = store->getDB()->remove_bucket(dpp, info);

    return ret;
  }

  int MotrBucket::remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) {
    return 0;
  }

  int MotrBucket::get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y)
  {
    int ret = 0;

//    ret = store->getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
//        &mtime, &bucket_version);

    return ret;
  }

  /* stats - Not for first pass */
  int MotrBucket::get_bucket_stats(const DoutPrefixProvider *dpp, int shard_id,
      std::string *bucket_ver, std::string *master_ver,
      std::map<RGWObjCategory, RGWStorageStats>& stats,
      std::string *max_marker, bool *syncstopped)
  {
    return 0;
  }

  int MotrBucket::get_bucket_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB *ctx)
  {
    return 0;
  }

  int MotrBucket::read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int MotrBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int MotrBucket::update_container_stats(const DoutPrefixProvider *dpp)
  {
    return 0;
  }

  int MotrBucket::check_bucket_shards(const DoutPrefixProvider *dpp)
  {
    return 0;
  }

  int MotrBucket::chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker)
  {
    int ret = 0;

    //ret = store->getDB()->update_bucket(dpp, "owner", info, false, &(new_user->get_id()), nullptr, nullptr, nullptr);

    /* XXX: Update policies of all the bucket->objects with new user */
    return ret;
  }

  int MotrBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
  {
    int ret = 0;

    //ret = store->getDB()->update_bucket(dpp, "info", info, exclusive, nullptr, nullptr, &_mtime, &info.objv_tracker);

    return ret;

  }

  int MotrBucket::remove_metadata(const DoutPrefixProvider* dpp, RGWObjVersionTracker* objv, optional_yield y)
  {
    /* XXX: same as MotrBUcket::remove_bucket() but should return error if there are objects
     * in that bucket. */

    //int ret = store->getDB()->remove_bucket(dpp, info);

    return 0;
  }

  /* Make sure to call get_bucket_info() if you need it first */
  bool MotrBucket::is_owner(User* user)
  {
    return (info.owner.compare(user->get_id()) == 0);
  }

  int MotrBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* XXX: Check if bucket contains any objects */
    return 0;
  }

  int MotrBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
      optional_yield y, bool check_size_only)
  {
    /* Not Handled in the first pass as stats are also needed */
    return 0;
  }

  int MotrBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& new_attrs, optional_yield y)
  {
    int ret = 0;

    Bucket::merge_and_store_attrs(dpp, new_attrs, y);

    /* XXX: handle has_instance_obj like in set_bucket_instance_attrs() */

    //ret = store->getDB()->update_bucket(dpp, "attrs", info, false, nullptr, &new_attrs, nullptr, &get_info().objv_tracker);

    return ret;
  }

  int MotrBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime)
  {
    int ret = 0;

//    ret = store->getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
//        pmtime, &bucket_version);

    return ret;
  }

  /* XXX: usage and stats not supported in the first pass */
  int MotrBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int MotrBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int MotrBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
  {
    /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
     * Delete all the object in the list from the object table of this
     * bucket
     */
    return 0;
  }

  int MotrBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
  {
    /* XXX: stats not supported yet */
    return 0;
  }

  int MotrBucket::rebuild_index(const DoutPrefixProvider *dpp)
  {
    /* there is no index table in dbstore. Not applicable */
    return 0;
  }

  int MotrBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
  {
    /* XXX: CHECK: set tag timeout for all the bucket objects? */
    return 0;
  }

  int MotrBucket::purge_instance(const DoutPrefixProvider *dpp)
  {
    /* XXX: CHECK: for dbstore only single instance supported.
     * Remove all the objects for that instance? Anything extra needed?
     */
    return 0;
  }

  int MotrBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
  {
    int ret = 0;
    bufferlist aclbl;

    acls = acl;
    acl.encode(aclbl);

    Attrs attrs = get_attrs();
    attrs[RGW_ATTR_ACL] = aclbl;

//    ret = store->getDB()->update_bucket(dpp, "attrs", info, false, &(acl.get_owner().get_id()), &attrs, nullptr, nullptr);

    return ret;
  }

  std::unique_ptr<Object> MotrBucket::get_object(const rgw_obj_key& k)
  {
    return std::make_unique<MotrObject>(this->store, k, this);
  }

  int MotrBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
  {
    int ret = 0;

//    results.objs.clear();
//
//    Motr::Bucket target(store->getDB(), get_info());
//    Motr::Bucket::List list_op(&target);
//
//    list_op.params.prefix = params.prefix;
//    list_op.params.delim = params.delim;
//    list_op.params.marker = params.marker;
//    list_op.params.ns = params.ns;
//    list_op.params.end_marker = params.end_marker;
//    list_op.params.ns = params.ns;
//    list_op.params.enforce_ns = params.enforce_ns;
//    list_op.params.access_list_filter = params.access_list_filter;
//    list_op.params.force_check_filter = params.force_check_filter;
//    list_op.params.list_versions = params.list_versions;
//    list_op.params.allow_unordered = params.allow_unordered;
//
//    results.objs.clear();
//    ret = list_op.list_objects(dpp, max, &results.objs, &results.common_prefixes, &results.is_truncated);
//    if (ret >= 0) {
//      results.next_marker = list_op.get_next_marker();
//      params.marker = results.next_marker;
//    }

    return ret;
  }

  int MotrBucket::list_multiparts(const DoutPrefixProvider *dpp,
				const string& prefix,
				string& marker,
				const string& delim,
				const int& max_uploads,
				vector<std::unique_ptr<MultipartUpload>>& uploads,
				map<string, bool> *common_prefixes,
				bool *is_truncated) {
    return 0;
  }

  int MotrBucket::abort_multiparts(const DoutPrefixProvider *dpp,
				 CephContext *cct,
				 string& prefix, string& delim) {
    return 0;
  }

  void MotrStore::finalize(void)
  {
  }

  const RGWZoneGroup& MotrZone::get_zonegroup()
  {
    return *zonegroup;
  }

  int MotrZone::get_zonegroup(const std::string& id, RGWZoneGroup& zg)
  {
    /* XXX: for now only one zonegroup supported */
    zg = *zonegroup;
    return 0;
  }

  const RGWZoneParams& MotrZone::get_params()
  {
    return *zone_params;
  }

  const rgw_zone_id& MotrZone::get_id()
  {
    return cur_zone_id;
  }

  const RGWRealm& MotrZone::get_realm()
  {
    return *realm;
  }

  const std::string& MotrZone::get_name() const
  {
    return zone_params->get_name();
  }

  bool MotrZone::is_writeable()
  {
    return true;
  }

  bool MotrZone::get_redirect_endpoint(std::string* endpoint)
  {
    return false;
  }

  bool MotrZone::has_zonegroup_api(const std::string& api) const
  {
    return false;
  }

  const std::string& MotrZone::get_current_period_id()
  {
    return current_period->get_id();
  }

  std::unique_ptr<LuaScriptManager> MotrStore::get_lua_script_manager()
  {
    return std::make_unique<MotrLuaScriptManager>(this);
  }

  int MotrObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh)
  {
//    if (!*state) {
//      *state = new RGWObjState();
//    }
//    Motr::Object op_target(store->getDB(), get_bucket()->get_info(), get_obj());
//    return op_target.get_obj_state(dpp, get_bucket()->get_info(), get_obj(), follow_olh, state);
    return 0;
  }

//  int MotrObject::read_attrs(const DoutPrefixProvider* dpp, Motr::Object::Read &read_op, optional_yield y, rgw_obj* target_obj)
//  {
//    read_op.params.attrs = &attrs;
//    read_op.params.target_obj = target_obj;
//    read_op.params.obj_size = &obj_size;
//    read_op.params.lastmod = &mtime;
//
//    return read_op.prepare(dpp);
//  }

  int MotrObject::set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj)
  {
//    Attrs empty;
//    Motr::Object op_target(store->getDB(),
//        get_bucket()->get_info(), target_obj ? *target_obj : get_obj());
//    return op_target.set_attrs(dpp, setattrs ? *setattrs : empty, delattrs);
    return 0;
  }

  int MotrObject::get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
  {
//    Motr::Object op_target(store->getDB(), get_bucket()->get_info(), get_obj());
//    Motr::Object::Read read_op(&op_target);
//
//    return read_attrs(dpp, read_op, y, target_obj);
    return 0;
  }

  int MotrObject::modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
  {
    rgw_obj target = get_obj();
    int r = get_obj_attrs(rctx, y, dpp, &target);
    if (r < 0) {
      return r;
    }
    set_atomic(rctx);
    attrs[attr_name] = attr_val;
    return set_obj_attrs(dpp, rctx, &attrs, nullptr, y, &target);
  }

  int MotrObject::delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y)
  {
    rgw_obj target = get_obj();
    Attrs rmattr;
    bufferlist bl;

    set_atomic(rctx);
    rmattr[attr_name] = bl;
    return set_obj_attrs(dpp, rctx, nullptr, &rmattr, y, &target);
  }

  int MotrObject::copy_obj_data(RGWObjectCtx& rctx, Bucket* dest_bucket,
      Object* dest_obj,
      uint16_t olh_epoch,
      std::string* petag,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    return 0;
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  void MotrObject::set_atomic(RGWObjectCtx* rctx) const
  {
    return;
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  void MotrObject::set_prefetch_data(RGWObjectCtx* rctx)
  {
    return;
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  void MotrObject::set_compressed(RGWObjectCtx* rctx)
  {
    return;
  }

  bool MotrObject::is_expired() {
    return false;
  }

  void MotrObject::gen_rand_obj_instance_name()
  {
     //store->getDB()->gen_rand_obj_instance_name(&key);
  }


  int MotrObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
      std::map<std::string, bufferlist> *m,
      bool* pmore, optional_yield y)
  {
//    Motr::Object op_target(store->getDB(),
//        get_bucket()->get_info(), get_obj());
//    return op_target.obj_omap_get_vals(dpp, marker, count, m, pmore);
    return 0;
  }

  int MotrObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
      optional_yield y)
  {
//    Motr::Object op_target(store->getDB(),
//        get_bucket()->get_info(), get_obj());
//    return op_target.obj_omap_get_all(dpp, m);
    return 0;
  }

  int MotrObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
      const std::set<std::string>& keys,
      Attrs* vals)
  {
//    Motr::Object op_target(store->getDB(),
//        get_bucket()->get_info(), get_obj());
//    return op_target.obj_omap_get_vals_by_keys(dpp, oid, keys, vals);
    return 0;
  }

  int MotrObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
      bool must_exist, optional_yield y)
  {
//    Motr::Object op_target(store->getDB(),
//        get_bucket()->get_info(), get_obj());
//    return op_target.obj_omap_set_val_by_key(dpp, key, val, must_exist);
    return 0;
  }

  MPSerializer* MotrObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
  {
    return nullptr;
  }

  int MotrObject::transition(RGWObjectCtx& rctx,
      Bucket* bucket,
      const rgw_placement_rule& placement_rule,
      const real_time& mtime,
      uint64_t olh_epoch,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    return 0;
  }

  bool MotrObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
  {
    /* XXX: support single default zone and zonegroup for now */
    return true;
  }

  int MotrObject::get_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx)
  {
    return 0;
  }

  std::unique_ptr<Object::ReadOp> MotrObject::get_read_op(RGWObjectCtx* ctx)
  {
    return std::make_unique<MotrObject::MotrReadOp>(this, ctx);
  }

  MotrObject::MotrReadOp::MotrReadOp(MotrObject *_source, RGWObjectCtx *_rctx) :
    source(_source),
    rctx(_rctx)
  { }

  int MotrObject::MotrReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
  {
    return 0;
  }

  int MotrObject::MotrReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
  {
    return 0;
  }

  int MotrObject::MotrReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
  {
    return 0;
  }

  std::unique_ptr<Object::DeleteOp> MotrObject::get_delete_op(RGWObjectCtx* ctx)
  {
    return std::make_unique<MotrObject::MotrDeleteOp>(this, ctx);
  }

  MotrObject::MotrDeleteOp::MotrDeleteOp(MotrObject *_source, RGWObjectCtx *_rctx) :
    source(_source),
    rctx(_rctx)
  { }

  int MotrObject::MotrDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
  {
    return 0;
  }

  int MotrObject::delete_object(const DoutPrefixProvider* dpp, RGWObjectCtx* obj_ctx, optional_yield y, bool prevent_versioning)
  {
//    Motr::Object del_target(/*store->getDB()*/NULL, bucket->get_info(), *obj_ctx, get_obj());
//    Motr::Object::Delete del_op(&del_target);
//
//    del_op.params.bucket_owner = bucket->get_info().owner;
//    del_op.params.versioning_status = bucket->get_info().versioning_status();
//
//    return del_op.delete_obj(dpp);
    return 0;
  }

  int MotrObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
      Completions* aio, bool keep_index_consistent,
      optional_yield y)
  {
    /* XXX: Make it async */
    return 0;
  }

  int MotrObject::copy_object(RGWObjectCtx& obj_ctx,
      User* user,
      req_info* info,
      const rgw_zone_id& source_zone,
      rgw::sal::Object* dest_object,
      rgw::sal::Bucket* dest_bucket,
      rgw::sal::Bucket* src_bucket,
      const rgw_placement_rule& dest_placement,
      ceph::real_time* src_mtime,
      ceph::real_time* mtime,
      const ceph::real_time* mod_ptr,
      const ceph::real_time* unmod_ptr,
      bool high_precision_time,
      const char* if_match,
      const char* if_nomatch,
      AttrsMod attrs_mod,
      bool copy_if_newer,
      Attrs& attrs,
      RGWObjCategory category,
      uint64_t olh_epoch,
      boost::optional<ceph::real_time> delete_at,
      std::string* version_id,
      std::string* tag,
      std::string* etag,
      void (*progress_cb)(off_t, void *),
      void* progress_data,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
        return 0;
  }

  int MotrObject::MotrReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y)
  {
    return 0;
  }

  int MotrObject::swift_versioning_restore(RGWObjectCtx* obj_ctx,
      bool& restored,
      const DoutPrefixProvider* dpp)
  {
    return 0;
  }

  int MotrObject::swift_versioning_copy(RGWObjectCtx* obj_ctx,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    return 0;
  }

  MotrAtomicWriter::MotrAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		        std::unique_ptr<rgw::sal::Object> _head_obj,
		        MotrStore* _store,
    		    const rgw_user& _owner, RGWObjectCtx& obj_ctx,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		        uint64_t _olh_epoch,
		        const std::string& _unique_tag) :
    			Writer(dpp, y),
	    		store(_store),
                owner(_owner),
                ptail_placement_rule(_ptail_placement_rule),
                olh_epoch(_olh_epoch),
                unique_tag(_unique_tag),
                obj(_store, _head_obj->get_key(), _head_obj->get_bucket()) {}

  int MotrAtomicWriter::prepare(optional_yield y)
  {
    return 0;
  }

  int MotrAtomicWriter::process(bufferlist&& data, uint64_t offset)
  {
//    total_data_size += data.length();
//
//    /* XXX: Optimize all bufferlist copies in this function */
//
//    /* copy head_data into meta. */
//    uint64_t head_size = store->getDB()->get_max_head_size();
//    unsigned head_len = 0;
//    uint64_t max_chunk_size = store->getDB()->get_max_chunk_size();
//    int excess_size = 0;
//
//    /* Accumulate tail_data till max_chunk_size or flush op */
//    bufferlist tail_data;
//
//    if (data.length() != 0) {
//      if (offset < head_size) {
//        /* XXX: handle case (if exists) where offset > 0 & < head_size */
//        head_len = std::min((uint64_t)data.length(),
//                                    head_size - offset);
//        bufferlist tmp;
//        data.begin(0).copy(head_len, tmp);
//        head_data.append(tmp);
//
//        //parent_op.meta.data = &head_data;
//        if (head_len == data.length()) {
//          return 0;
//        }
//
//        /* Move offset by copy_len */
//        offset = head_len;
//      }
//
//      /* handle tail parts.
//       * First accumulate and write data into dbstore in its chunk_size
//       * parts
//       */
//      if (!tail_part_size) { /* new tail part */
//        tail_part_offset = offset;
//      }
//      data.begin(head_len).copy(data.length() - head_len, tail_data);
//      tail_part_size += tail_data.length();
//      tail_part_data.append(tail_data);
//
//      if (tail_part_size < max_chunk_size)  {
//        return 0;
//      } else {
//        int write_ofs = 0;
//        while (tail_part_size >= max_chunk_size) {
//          excess_size = tail_part_size - max_chunk_size;
//          bufferlist tmp;
//          tail_part_data.begin(write_ofs).copy(max_chunk_size, tmp);
//          /* write tail objects data */
////          int ret = parent_op.write_data(dpp, tmp, tail_part_offset);
////
////          if (ret < 0) {
////            return ret;
////          }
//
//          tail_part_size -= max_chunk_size;
//          write_ofs += max_chunk_size;
//          tail_part_offset += max_chunk_size;
//        }
//        /* reset tail parts or update if excess data */
//        if (excess_size > 0) { /* wrote max_chunk_size data */
//          tail_part_size = excess_size;
//          bufferlist tmp;
//          tail_part_data.begin(write_ofs).copy(excess_size, tmp);
//          tail_part_data = tmp;
//        } else {
//          tail_part_size = 0;
//          tail_part_data.clear();
//          tail_part_offset = 0;
//        }
//      }
//    } else {
//      if (tail_part_size == 0) {
//        return 0; /* nothing more to write */
//      }
//
//      /* flush watever tail data is present */
////      int ret = parent_op.write_data(dpp, tail_part_data, tail_part_offset);
////      if (ret < 0) {
////        return ret;
////      }
//      tail_part_size = 0;
//      tail_part_data.clear();
//      tail_part_offset = 0;
//    }

    return 0;
  }

  int MotrAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                         ceph::real_time *mtime, ceph::real_time set_mtime,
                         std::map<std::string, bufferlist>& attrs,
                         ceph::real_time delete_at,
                         const char *if_match, const char *if_nomatch,
                         const std::string *user_data,
                         rgw_zone_set *zones_trace, bool *canceled,
                         optional_yield y)
  {
//    parent_op.meta.mtime = mtime;
//    parent_op.meta.delete_at = delete_at;
//    parent_op.meta.if_match = if_match;
//    parent_op.meta.if_nomatch = if_nomatch;
//    parent_op.meta.user_data = user_data;
//    parent_op.meta.zones_trace = zones_trace;
//    
//    /* XXX: handle accounted size */
//    accounted_size = total_data_size;
//    int ret = parent_op.write_meta(dpp, total_data_size, accounted_size, attrs);
//    if (canceled) {
//      *canceled = parent_op.meta.canceled;
//    }

    return 0;

  }

  std::unique_ptr<RGWRole> MotrStore::get_role(std::string name,
      std::string tenant,
      std::string path,
      std::string trust_policy,
      std::string max_session_duration_str,
      std::multimap<std::string,std::string> tags)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  std::unique_ptr<RGWRole> MotrStore::get_role(std::string id)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  int MotrStore::get_roles(const DoutPrefixProvider *dpp,
      optional_yield y,
      const std::string& path_prefix,
      const std::string& tenant,
      vector<std::unique_ptr<RGWRole>>& roles)
  {
    return 0;
  }

  std::unique_ptr<RGWOIDCProvider> MotrStore::get_oidc_provider()
  {
    RGWOIDCProvider* p = nullptr;
    return std::unique_ptr<RGWOIDCProvider>(p);
  }

  int MotrStore::get_oidc_providers(const DoutPrefixProvider *dpp,
      const std::string& tenant,
      vector<std::unique_ptr<RGWOIDCProvider>>& providers)
  {
    return 0;
  }

  std::unique_ptr<MultipartUpload> MotrStore::get_multipart_upload(Bucket* bucket, const std::string& oid, std::optional<std::string> upload_id, ceph::real_time mtime) {
    return nullptr;
  }

  std::unique_ptr<Writer> MotrStore::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) {
    return nullptr;
  }

  std::unique_ptr<Writer> MotrStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) {
    return std::make_unique<MotrAtomicWriter>(dpp, y,
                    std::move(_head_obj), this, owner, obj_ctx,
                    ptail_placement_rule, olh_epoch, unique_tag);
  }

  std::unique_ptr<User> MotrStore::get_user(const rgw_user &u)
  {
    return std::make_unique<MotrUser>(this, u);
  }

  int MotrStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
  {
//    RGWUserInfo uinfo;
//    User *u;
//    int ret = 0;
//    RGWObjVersionTracker objv_tracker;
//
//    ret = getDB()->get_user(dpp, string("access_key"), key, uinfo, nullptr,
//        &objv_tracker);
//
//    if (ret < 0)
//      return ret;
//
//    u = new MotrUser(this, uinfo);
//
//    if (!u)
//      return -ENOMEM;
//
//    u->get_version_tracker() = objv_tracker;
//    user->reset(u);

    return 0;
  }

  int MotrStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
  {
//    RGWUserInfo uinfo;
//    User *u;
//    int ret = 0;
//    RGWObjVersionTracker objv_tracker;
//
//    ret = getDB()->get_user(dpp, string("email"), email, uinfo, nullptr,
//        &objv_tracker);
//
//    if (ret < 0)
//      return ret;
//
//    u = new MotrUser(this, uinfo);
//
//    if (!u)
//      return -ENOMEM;
//
//    u->get_version_tracker() = objv_tracker;
//    user->reset(u);
//
//    return ret;
    return 0;
  }

  int MotrStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
  {
    /* Swift keys and subusers are not supported for now */
    return 0;
  }

  std::unique_ptr<Object> MotrStore::get_object(const rgw_obj_key& k)
  {
    return std::make_unique<MotrObject>(this, k);
  }


  int MotrStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    int ret;
    Bucket* bp;

    bp = new MotrBucket(this, b, u);
    ret = bp->get_bucket_info(dpp, y);
    if (ret < 0) {
      delete bp;
      return ret;
    }

    bucket->reset(bp);
    return 0;
  }

  int MotrStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
  {
    Bucket* bp;

    bp = new MotrBucket(this, i, u);
    /* Don't need to fetch the bucket info, use the provided one */

    bucket->reset(bp);
    return 0;
  }

  int MotrStore::get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    rgw_bucket b;

    b.tenant = tenant;
    b.name = name;

    return get_bucket(dpp, u, b, bucket, y);
  }

  int MotrStore::create_bucket(const DoutPrefixProvider *dpp,
      User* u, const rgw_bucket& b,
      const string& zonegroup_id,
      rgw_placement_rule& placement_rule,
      string& swift_ver_location,
      const RGWQuotaInfo * pquota_info,
      const RGWAccessControlPolicy& policy,
      Attrs& attrs,
      RGWBucketInfo& info,
      obj_version& ep_objv,
      bool exclusive,
      bool obj_lock_enabled,
      bool *existed,
      req_info& req_info,
      std::unique_ptr<Bucket>* bucket_out,
      optional_yield y)
  {
//    int ret;
//    bufferlist in_data;
//    RGWBucketInfo master_info;
//    rgw_bucket *pmaster_bucket = nullptr;
//    uint32_t *pmaster_num_shards = nullptr;
//    real_time creation_time;
//    std::unique_ptr<Bucket> bucket;
//    obj_version objv, *pobjv = NULL;
//
//    /* If it exists, look it up; otherwise create it */
//    ret = get_bucket(dpp, u, b, &bucket, y);
//    if (ret < 0 && ret != -ENOENT)
//      return ret;
//
//    if (ret != -ENOENT) {
//      RGWAccessControlPolicy old_policy(ctx());
//      *existed = true;
//      if (swift_ver_location.empty()) {
//        swift_ver_location = bucket->get_info().swift_ver_location;
//      }
//      placement_rule.inherit_from(bucket->get_info().placement_rule);
//
//      // don't allow changes to the acl policy
//      /*    int r = rgw_op_get_bucket_policy_from_attr(dpp, this, u, bucket->get_attrs(),
//            &old_policy, y);
//            if (r >= 0 && old_policy != policy) {
//            bucket_out->swap(bucket);
//            return -EEXIST;
//            }*/
//    } else {
//      bucket = std::make_unique<MotrBucket>(this, b, u);
//      *existed = false;
//      bucket->set_attrs(attrs);
//      // XXX: For now single default zone and STANDARD storage class
//      // supported.
//      placement_rule.name = "default";
//      placement_rule.storage_class = "STANDARD";
//    }
//
//    /*
//     * XXX: If not master zone, fwd the request to master zone.
//     * For now MotrStore has single zone.
//     */
//    std::string zid = zonegroup_id;
//    /* if (zid.empty()) {
//       zid = svc()->zone->get_zonegroup().get_id();
//       } */
//
//    if (*existed) {
//      rgw_placement_rule selected_placement_rule;
//      /* XXX: Handle this when zone is implemented
//         ret = svc()->zone->select_bucket_placement(u.get_info(),
//         zid, placement_rule,
//         &selected_placement_rule, nullptr, y);
//         if (selected_placement_rule != info.placement_rule) {
//         ret = -EEXIST;
//         bucket_out->swap(bucket);
//         return ret;
//         } */
//    } else {
//
//      /* XXX: We may not need to send all these params. Cleanup the unused ones */
//      ret = getDB()->create_bucket(dpp, u->get_info(), bucket->get_key(),
//          zid, placement_rule, swift_ver_location, pquota_info,
//          attrs, info, pobjv, &ep_objv, creation_time,
//          pmaster_bucket, pmaster_num_shards, y, exclusive);
//      if (ret == -EEXIST) {
//        *existed = true;
//        ret = 0;
//      } else if (ret != 0) {
//        return ret;
//      }
//    }
//
//    bucket->set_version(ep_objv);
//    bucket->get_info() = info;
//
//    bucket_out->swap(bucket);
//
    //return ret;
    return 0;
  }

  bool MotrStore::is_meta_master()
  {
    return true;
  }

  int MotrStore::forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version *objv,
      bufferlist& in_data,
      JSONParser *jp, req_info& info,
      optional_yield y)
  {
    return 0;
  }

  int MotrStore::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, Bucket* bucket, Object* obj, optional_yield y)
  {
    return 0;
  }

  std::string MotrStore::zone_unique_id(uint64_t unique_num)
  {
    return "";
  }

  std::string MotrStore::zone_unique_trans_id(const uint64_t unique_num)
  {
    return "";
  }

  int MotrStore::cluster_stat(RGWClusterStat& stats)
  {
    return 0;
  }

  std::unique_ptr<Lifecycle> MotrStore::get_lifecycle(void)
  {
    return 0;
  }

  std::unique_ptr<Completions> MotrStore::get_completions(void)
  {
    return 0;
  }

  std::unique_ptr<Notification> MotrStore::get_notification(rgw::sal::Object* obj,
      struct req_state* s,
      rgw::notify::EventType event_type, const std::string* object_name)
  {
    return std::make_unique<MotrNotification>(obj, event_type);
  }

  int MotrStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info)
  {
    return 0;
  }

  int MotrStore::log_op(const DoutPrefixProvider *dpp, string& oid, bufferlist& bl)
  {
    return 0;
  }

  int MotrStore::register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
      const map<string, string>& meta)
  {
    return 0;
  }

  void MotrStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
  {
    // XXX: Not handled for the first pass 
    return;
  }

  int MotrStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled)
  {
    int ret = 0;

//    vector<rgw_bucket>::iterator iter;
//
//    for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
//      rgw_bucket& bucket = *iter;
//      if (enabled) {
//        ldpp_dout(dpp, 20) << "enabling bucket name=" << bucket.name << dendl;
//      } else {
//        ldpp_dout(dpp, 20) << "disabling bucket name=" << bucket.name << dendl;
//      }
//
//      RGWBucketInfo info;
//      map<string, bufferlist> attrs;
//      int r = getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
//          nullptr, nullptr);
//      if (r < 0) {
//        ldpp_dout(dpp, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
//        ret = r;
//        continue;
//      }
//      if (enabled) {
//        info.flags &= ~BUCKET_SUSPENDED;
//      } else {
//        info.flags |= BUCKET_SUSPENDED;
//      }
//
//      r = getDB()->update_bucket(dpp, "info", info, false, nullptr, &attrs, nullptr, &info.objv_tracker);
//      if (r < 0) {
//        ldpp_dout(dpp, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
//        ret = r;
//        continue;
//      }
//    }
    return ret;
  }

  int MotrStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
      std::optional<rgw_zone_id> zone,
      std::optional<rgw_bucket> bucket,
      RGWBucketSyncPolicyHandlerRef *phandler,
      optional_yield y)
  {
    return 0;
  }

  RGWDataSyncStatusManager* MotrStore::get_data_sync_manager(const rgw_zone_id& source_zone)
  {
    return 0;
  }

  int MotrStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, 
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int MotrStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    return 0;
  }

  int MotrStore::get_config_key_val(string name, bufferlist *bl)
  {
    return 0;
  }

  int MotrStore::meta_list_keys_init(const DoutPrefixProvider *dpp, const string& section, const string& marker, void** phandle)
  {
    return 0;
  }

  int MotrStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<string>& keys, bool* truncated)
  {
    return 0;
  }

  void MotrStore::meta_list_keys_complete(void* handle)
  {
    return;
  }

  std::string MotrStore::meta_get_marker(void* handle)
  {
    return "";
  }

  int MotrStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
  {
    return 0;
  }

  int MotrStore::open_idx(struct m0_uint128 *id, bool create, struct m0_idx *idx)
  {
    m0_idx_init(idx, &container.co_realm, id);

    if (!create)
      return 0; // nothing to do more

    // create index or make sure it's created
    struct m0_op *op;
    int rc = m0_entity_create(NULL, &idx->in_entity, &op);
    if (rc != 0) {
      ldout(cctx, 0) << "ERROR: m0_entity_create() failed: " << rc << dendl;
      goto out;
    }

    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);

    if (rc != 0 && rc != -EEXIST)
      ldout(cctx, 0) << "ERROR: index create failed: " << rc << dendl;
  out:
    return rc;
  }

  static void set_m0bufvec(struct m0_bufvec *bv, vector<byte> *vec)
  {
    *bv->ov_buf = reinterpret_cast<char*>(vec->data());
    *bv->ov_vec.v_count = vec->size();
  }

  // idx must be opened with open_idx() beforehand
  int MotrStore::do_idx_op(struct m0_idx *idx, enum m0_idx_opcode opcode,
                           vector<byte> *key, vector<byte> *val, bool update)
  {
    int rc, rc_i;
    struct m0_bufvec k, v, *vp = &v;
    uint32_t flags = 0;
    struct m0_op *op;

    if (m0_bufvec_empty_alloc(&k, 1) != 0) {
      ldout(cctx, 0) << "ERROR: failed to allocate key bufvec" << dendl;
      return -ENOMEM;
    }

    if (opcode == M0_IC_PUT || opcode == M0_IC_GET) {
      rc = -ENOMEM;
      if (m0_bufvec_empty_alloc(&v, 1) != 0) {
        ldout(cctx, 0) << "ERROR: failed to allocate value bufvec" << dendl;
        goto out;
      }
    }

    set_m0bufvec(&k, key);
    if (opcode == M0_IC_PUT)
      set_m0bufvec(&v, val);

    if (opcode == M0_IC_DEL)
      vp = NULL;

    if (opcode == M0_IC_PUT && update)
      flags |= M0_OIF_OVERWRITE;

    rc = m0_idx_op(idx, opcode, &k, vp, &rc_i, flags, &op);
    if (rc != 0) {
      ldout(cctx, 0) << "ERROR: failed to init index op: " << rc << dendl;
      goto out;
    }

    m0_op_launch(&op, 1);
    rc = m0_op_wait(op, M0_BITS(M0_OS_FAILED, M0_OS_STABLE), M0_TIME_NEVER) ?:
         m0_rc(op);
    m0_op_fini(op);
    m0_op_free(op);

    if (rc != 0) {
      ldout(cctx, 0) << "ERROR: op failed: " << rc << dendl;
      goto out;
    }

    if (rc_i != 0) {
      ldout(cctx, 0) << "ERROR: idx op failed: " << rc_i << dendl;
      goto out;
    }

    if (opcode == M0_IC_GET) {
      val->resize(*v.ov_vec.v_count);
      memcpy(reinterpret_cast<char*>(val->data()), *v.ov_buf, *v.ov_vec.v_count);
    }

  out:
    m0_bufvec_free2(&k);
    if (opcode == M0_IC_GET)
      m0_bufvec_free(&v); // cleanup buffer after GET
    else if (opcode == M0_IC_PUT)
      m0_bufvec_free2(&v);

    return rc;
  }
} // namespace rgw::sal

extern "C" {

  void *newMotrStore(CephContext *cct)
  {
    int rc = -1;
    rgw::sal::MotrStore *store = new rgw::sal::MotrStore(cct);

    if (store) {
      store->conf.mc_is_oostore     = true;
      // XXX: these params should be taken from config settings and
      // cct somehow?
      store->conf.mc_local_addr     = "192.168.180.182@tcp:12345:4:1";
      store->conf.mc_ha_addr        = "192.168.180.182@tcp:12345:1:1";
      store->conf.mc_profile        = "0x7000000000000001:0x4f";
      store->conf.mc_process_fid    = "0x7200000000000001:0x29";
      store->conf.mc_tm_recv_queue_min_len =    64;
      store->conf.mc_max_rpc_msg_size      = 65536;
      store->conf.mc_idx_service_id  = M0_IDX_DIX;
      store->dix_conf.kc_create_meta = false;
      store->conf.mc_idx_service_conf = &store->dix_conf;

      rc = m0_client_init(&store->instance, &store->conf, true);
      if (rc != 0) {
	ldout(cct, 0) << "ERROR: m0_client_init() failed: " << rc << dendl;
	goto out;
      }

      m0_container_init(&store->container, nullptr, &M0_UBER_REALM, store->instance);
      rc = store->container.co_realm.re_entity.en_sm.sm_rc;
      if (rc != 0) {
	ldout(cct, 0) << "ERROR: m0_container_init() failed: " << rc << dendl;
	goto out;
      }
    }

out:
    if (rc != 0) {
      delete store;
      return nullptr;
    }
    return store;
  }

}
