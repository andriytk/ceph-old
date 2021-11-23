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

#pragma once

#include <string>
#include <map>
#include <vector>
#include <set>
#include <list>
using std::string;
using std::map;
using std::vector;
using std::set;
using std::list;

extern "C" {
#include "motr/config.h"
#include "motr/client.h"
}

#include "rgw_sal.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"

namespace rgw { namespace sal {

  class MotrStore;

  // Global Motr indices
  #define RGW_MOTR_USERS_IDX_NAME   "motr.rgw.users"
  #define RGW_MOTR_BUCKET_INST_IDX_NAME "motr.rgw.bucket.instances"
  #define RGW_MOTR_BUCKET_HD_IDX_NAME   "motr.rgw.bucket.headers"
  //#define RGW_MOTR_BUCKET_ACL_IDX_NAME  "motr.rgw.bucket.acls"

  std::string motr_global_indices[] = {
    RGW_MOTR_USERS_IDX_NAME,
    RGW_MOTR_BUCKET_INST_IDX_NAME,
    RGW_MOTR_BUCKET_HD_IDX_NAME
  };

  struct MotrUserInfo {
    RGWUserInfo info;
    obj_version user_version;
    rgw::sal::Attrs attrs;

    void encode(bufferlist& bl)  const
    {
      ENCODE_START(3, 3, bl);
      encode(info, bl);
      encode(user_version, bl);
      encode(attrs, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl)
    {
      DECODE_START(3, bl);
      decode(info, bl);
      decode(user_version, bl);
      decode(attrs, bl);
      DECODE_FINISH(bl);
    }
  };
  WRITE_CLASS_ENCODER(MotrUserInfo);

class MotrNotification : public Notification {
protected:
  Object* obj;
  rgw::notify::EventType event_type;

  public:
    MotrNotification(Object* _obj, rgw::notify::EventType _type) : Notification(_obj, _type), obj(_obj), event_type(_type) {}
    ~MotrNotification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override { return 0;}
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const string& etag, const string& version) override { return 0; }
};

  class MotrUser : public User {
    private:
      MotrStore         *store;
      struct m0_uint128  idxID = {0xe5ecb53640d4ecce, 0x6a156cd5a74aa3b8}; // MD5 of “motr.rgw.users“
      struct m0_idx      idx;

    public:
      MotrUser(MotrStore *_st, const rgw_user& _u) : User(_u), store(_st) { }
      MotrUser(MotrStore *_st, const RGWUserInfo& _i) : User(_i), store(_st) { }
      MotrUser(MotrStore *_st) : store(_st) { }
      MotrUser(MotrUser& _o) = default;
      MotrUser() {}

      virtual std::unique_ptr<User> clone() override {
        return std::unique_ptr<User>(new MotrUser(*this));
      }
      int list_buckets(const DoutPrefixProvider *dpp, const string& marker, const string& end_marker,
          uint64_t max, bool need_stats, BucketList& buckets, optional_yield y) override;
      virtual Bucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) override;
      virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int read_stats(const DoutPrefixProvider *dpp,
          optional_yield y, RGWStorageStats* stats,
          ceph::real_time *last_stats_sync = nullptr,
          ceph::real_time *last_stats_update = nullptr) override;
      virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) override;
      virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool* is_truncated, RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;

      /* Placeholders */
      int load_user_from_motr_idx(const DoutPrefixProvider *dpp,
                                  RGWUserInfo& info, map<string, bufferlist> *attrs,
                                  RGWObjVersionTracker *objv_tracker);
      virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) override;
      virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;

      int create_user_info_idx();

      friend class MotrBucket;
  };

  class MotrBucket : public Bucket {
    private:
      MotrStore *store;
      RGWAccessControlPolicy acls;

    // RGWBucketInfo and other information that are shown when listing a bucket is
    // represented in struct MotrBucketInfo. The structure is encoded and stored
    // as the value of the global bucket instance index.
    // TODO: compare pros and cons of separating the bucket_attrs (ACLs, tag etc.)
    // into a different index.
    struct MotrBucketInfo {
      RGWBucketInfo info;

      obj_version bucket_version;
      ceph::real_time mtime;

      rgw::sal::Attrs bucket_attrs;

      void encode(bufferlist& bl)  const
      {
        ENCODE_START(4, 4, bl);
        encode(info, bl);
        encode(bucket_version, bl);
        encode(mtime, bl);
        encode(bucket_attrs, bl); //rgw_cache.h example for a map
        ENCODE_FINISH(bl);
      }

      void decode(bufferlist::const_iterator& bl)
      {
        DECODE_START(4, bl);
        decode(info, bl);
        decode(bucket_version, bl);
        decode(mtime, bl);
        decode(bucket_attrs, bl);
        DECODE_FINISH(bl);
      }
    };
    WRITE_CLASS_ENCODER(MotrBucketInfo);

    public:
      MotrBucket(MotrStore *_st)
        : store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, User* _u)
        : Bucket(_u),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const rgw_bucket& _b)
        : Bucket(_b),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const RGWBucketEnt& _e)
        : Bucket(_e),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const RGWBucketInfo& _i)
        : Bucket(_i),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const rgw_bucket& _b, User* _u)
        : Bucket(_b, _u),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const RGWBucketEnt& _e, User* _u)
        : Bucket(_e, _u),
        store(_st),
        acls() {
        }

      MotrBucket(MotrStore *_st, const RGWBucketInfo& _i, User* _u)
        : Bucket(_i, _u),
        store(_st),
        acls() {
        }

      ~MotrBucket() { }

      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
      virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, string prefix, string delimiter, bool forward_to_master, req_info* req_info, optional_yield y) override;
      virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) override;
      virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
      virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
      int put_bucket_info(const DoutPrefixProvider *dpp, optional_yield y);
      virtual int get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y) override;
      int link_user(const DoutPrefixProvider* dpp, User* new_user, optional_yield y);
      int unlink_user(const DoutPrefixProvider* dpp, User* new_user, optional_yield y);
      int create_bucket_index();
      virtual int get_bucket_stats(const DoutPrefixProvider *dpp, int shard_id,
          string *bucket_ver, string *master_ver,
          map<RGWObjCategory, RGWStorageStats>& stats,
          string *max_marker = nullptr,
          bool *syncstopped = nullptr) override;
      virtual int get_bucket_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx) override;
      virtual int read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
      virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
      virtual int chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const string* marker = nullptr) override;
      virtual int put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime) override;
      virtual int remove_metadata(const DoutPrefixProvider* dpp, RGWObjVersionTracker* objv, optional_yield y) override;
      virtual bool is_owner(User* user) override;
      virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
      virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y) override;
      virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool *is_truncated, RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
      virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
      virtual int check_index(const DoutPrefixProvider *dpp, map<RGWObjCategory, RGWStorageStats>& existing_stats, map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
      virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
      virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
      virtual int purge_instance(const DoutPrefixProvider *dpp) override;
      virtual std::unique_ptr<Bucket> clone() override {
        return std::make_unique<MotrBucket>(*this);
      }
      virtual int list_multiparts(const DoutPrefixProvider *dpp,
				const string& prefix,
				string& marker,
				const string& delim,
				const int& max_uploads,
				vector<std::unique_ptr<MultipartUpload>>& uploads,
				map<string, bool> *common_prefixes,
				bool *is_truncated) override;
      virtual int abort_multiparts(const DoutPrefixProvider *dpp,
				 CephContext *cct,
				 string& prefix, string& delim) override;

      friend class MotrStore;
  };

  class MotrZone : public Zone {
    protected:
      MotrStore* store;
      RGWRealm *realm{nullptr};
      RGWZoneGroup *zonegroup{nullptr};
      RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */
      RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
      RGWPeriod *current_period{nullptr};
      rgw_zone_id cur_zone_id;

    public:
      MotrZone(MotrStore* _store) : store(_store) {
        realm = new RGWRealm();
        zonegroup = new RGWZoneGroup();
        zone_public_config = new RGWZone();
        zone_params = new RGWZoneParams();
        current_period = new RGWPeriod();
        cur_zone_id = rgw_zone_id(zone_params->get_id());

        // XXX: only default and STANDARD supported for now
        RGWZonePlacementInfo info;
        RGWZoneStorageClasses sc;
        sc.set_storage_class("STANDARD", nullptr, nullptr);
        info.storage_classes = sc;
        zone_params->placement_pools["default"] = info;
      }
      ~MotrZone() = default;

      virtual const RGWZoneGroup& get_zonegroup() override;
      virtual int get_zonegroup(const string& id, RGWZoneGroup& zonegroup) override;
      virtual const RGWZoneParams& get_params() override;
      virtual const rgw_zone_id& get_id() override;
      virtual const RGWRealm& get_realm() override;
      virtual const string& get_name() const override;
      virtual bool is_writeable() override;
      virtual bool get_redirect_endpoint(string* endpoint) override;
      virtual bool has_zonegroup_api(const string& api) const override;
      virtual const string& get_current_period_id() override;
  };

  class MotrLuaScriptManager : public LuaScriptManager {
    MotrStore* store;

    public:
    MotrLuaScriptManager(MotrStore* _s) : store(_s)
    {
    }
    virtual ~MotrLuaScriptManager() = default;

    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const string& key, string& script) override { return -ENOENT; }
    virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const string& key, const string& script) override { return -ENOENT; }
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const string& key) override { return -ENOENT; }
  };

  class MotrOIDCProvider : public RGWOIDCProvider {
    MotrStore* store;
    public:
    MotrOIDCProvider(MotrStore* _store) : store(_store) {}
    ~MotrOIDCProvider() = default;

    virtual int store_url(const DoutPrefixProvider *dpp, const string& url, bool exclusive, optional_yield y) override { return 0; }
    virtual int read_url(const DoutPrefixProvider *dpp, const string& url, const string& tenant) override { return 0; }
    virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override { return 0;}

    void encode(bufferlist& bl) const {
      RGWOIDCProvider::encode(bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      RGWOIDCProvider::decode(bl);
    }
  };

  class MotrObject : public Object {
    private:
      MotrStore* store;
      RGWAccessControlPolicy acls;
      /* XXX: to be removed. Till Dan's patch comes, a placeholder
       * for RGWObjState
       */
      RGWObjState* state;

      uint64_t       layout_id;

    public:

      struct m0_obj *mobj = NULL;

      struct MotrReadOp : public ReadOp {
        private:
          MotrObject* source;
          RGWObjectCtx* rctx;

        public:
          MotrReadOp(MotrObject *_source, RGWObjectCtx *_rctx);

          virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
          virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) override;
          virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) override;
          virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override;
      };

      struct MotrDeleteOp : public DeleteOp {
        private:
          MotrObject* source;
          RGWObjectCtx* rctx;

        public:
          MotrDeleteOp(MotrObject* _source, RGWObjectCtx* _rctx);

          virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
      };

      MotrObject() = default;

      MotrObject(MotrStore *_st, const rgw_obj_key& _k)
        : Object(_k),
        store(_st),
        acls() {
        }
      MotrObject(MotrStore *_st, const rgw_obj_key& _k, Bucket* _b)
        : Object(_k, _b),
        store(_st),
        acls() {
        }
      MotrObject(MotrObject& _o) = default;

      virtual int delete_object(const DoutPrefixProvider* dpp,
          RGWObjectCtx* obj_ctx,
          optional_yield y,
          bool prevent_versioning = false) override;
      virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
          bool keep_index_consistent, optional_yield y) override;
      virtual int copy_object(RGWObjectCtx& obj_ctx, User* user,
          req_info* info, const rgw_zone_id& source_zone,
          rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
          rgw::sal::Bucket* src_bucket,
          const rgw_placement_rule& dest_placement,
          ceph::real_time* src_mtime, ceph::real_time* mtime,
          const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
          bool high_precision_time,
          const char* if_match, const char* if_nomatch,
          AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
          RGWObjCategory category, uint64_t olh_epoch,
          boost::optional<ceph::real_time> delete_at,
          string* version_id, string* tag, string* etag,
          void (*progress_cb)(off_t, void *), void* progress_data,
          const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
      virtual int set_acl(const RGWAccessControlPolicy& acl) override { acls = acl; return 0; }
      virtual void set_atomic(RGWObjectCtx* rctx) const override;
      virtual void set_prefetch_data(RGWObjectCtx* rctx) override;
      virtual void set_compressed(RGWObjectCtx* rctx) override;

      virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
      virtual int set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj = NULL) override;
      virtual int get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
      virtual int modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
      virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y) override;
      virtual int copy_obj_data(RGWObjectCtx& rctx, Bucket* dest_bucket, Object* dest_obj, uint16_t olh_epoch, string* petag, const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual bool is_expired() override;
      virtual void gen_rand_obj_instance_name() override;
      virtual std::unique_ptr<Object> clone() override {
        return std::unique_ptr<Object>(new MotrObject(*this));
      }
      virtual MPSerializer* get_serializer(const DoutPrefixProvider *dpp, const string& lock_name) override;
      virtual int transition(RGWObjectCtx& rctx,
          Bucket* bucket,
          const rgw_placement_rule& placement_rule,
          const real_time& mtime,
          uint64_t olh_epoch,
          const DoutPrefixProvider* dpp,
          optional_yield y) override;
      virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
      virtual int get_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx) override;

      /* Swift versioning */
      virtual int swift_versioning_restore(RGWObjectCtx* obj_ctx,
          bool& restored,
          const DoutPrefixProvider* dpp) override;
      virtual int swift_versioning_copy(RGWObjectCtx* obj_ctx,
          const DoutPrefixProvider* dpp,
          optional_yield y) override;

      /* OPs */
      virtual std::unique_ptr<ReadOp> get_read_op(RGWObjectCtx *) override;
      virtual std::unique_ptr<DeleteOp> get_delete_op(RGWObjectCtx*) override;

      /* OMAP */
      virtual int omap_get_vals(const DoutPrefixProvider *dpp, const string& marker, uint64_t count,
          map<string, bufferlist> *m,
          bool* pmore, optional_yield y) override;
      virtual int omap_get_all(const DoutPrefixProvider *dpp, map<string, bufferlist> *m,
          optional_yield y) override;
      virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const string& oid,
          const std::set<string>& keys,
          Attrs* vals) override;
      virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const string& key, bufferlist& val,
          bool must_exist, optional_yield y) override;
    private:
      //int read_attrs(const DoutPrefixProvider* dpp, Motr::Object::Read &read_op, optional_yield y, rgw_obj* target_obj = nullptr);

    public:
      bool is_opened() { return mobj != NULL; }
      int create_mobj(const DoutPrefixProvider *dpp, uint64_t sz);
      void close_mobj();
      unsigned get_optimal_bs(unsigned len);
  };

  class MotrAtomicWriter : public Writer {
    protected:
    rgw::sal::MotrStore* store;
    const rgw_user& owner;
    const rgw_placement_rule *ptail_placement_rule;
    uint64_t olh_epoch;
    const string& unique_tag;
    MotrObject obj;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

    struct m0_bufvec buf;
    struct m0_bufvec attr;
    struct m0_indexvec ext;

    public:
    MotrAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		        std::unique_ptr<rgw::sal::Object> _head_obj,
		        MotrStore* _store,
    		    const rgw_user& _owner, RGWObjectCtx& obj_ctx,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		        uint64_t _olh_epoch,
		        const string& _unique_tag);
    ~MotrAtomicWriter() = default;

    // prepare to start processing object data
    virtual int prepare(optional_yield y) override;

    // Process a bufferlist
    virtual int process(bufferlist&& data, uint64_t offset) override;

    // complete the operation and make its result visible to clients
    virtual int complete(size_t accounted_size, const string& etag,
                         ceph::real_time *mtime, ceph::real_time set_mtime,
                         map<string, bufferlist>& attrs,
                         ceph::real_time delete_at,
                         const char *if_match, const char *if_nomatch,
                         const string *user_data,
                         rgw_zone_set *zones_trace, bool *canceled,
                         optional_yield y) override;
    void cleanup();
  };

  class MotrStore : public Store {
    private:
      string luarocks_path;
      MotrZone zone;
      RGWSyncModuleInstanceRef sync_module;

    public:
      CephContext *cctx;
      struct m0_client   *instance;
      struct m0_container container;
      struct m0_realm     uber_realm;
      struct m0_config    conf = {};
      struct m0_idx_dix_config dix_conf = {};

      MotrStore(CephContext *c): zone(this), cctx(c) {}
      ~MotrStore() { ; }

      virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
      virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const string& key, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_email(const DoutPrefixProvider *dpp, const string& email, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
      virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const string& tenant, const string&name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual int create_bucket(const DoutPrefixProvider* dpp,
          User* u, const rgw_bucket& b,
          const string& zonegroup_id,
          rgw_placement_rule& placement_rule,
          string& swift_ver_location,
          const RGWQuotaInfo* pquota_info,
          const RGWAccessControlPolicy& policy,
          Attrs& attrs,
          RGWBucketInfo& info,
          obj_version& ep_objv,
          bool exclusive,
          bool obj_lock_enabled,
          bool* existed,
          req_info& req_info,
          std::unique_ptr<Bucket>* bucket,
          optional_yield y) override;
      virtual bool is_meta_master() override;
      virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
          bufferlist& in_data, JSONParser *jp, req_info& info,
          optional_yield y) override;
      virtual int defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, Bucket* bucket, Object* obj,
          optional_yield y) override;
      virtual Zone* get_zone() { return &zone; }
      virtual string zone_unique_id(uint64_t unique_num) override;
      virtual string zone_unique_trans_id(const uint64_t unique_num) override;
      virtual int cluster_stat(RGWClusterStat& stats) override;
      virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
      virtual std::unique_ptr<Completions> get_completions(void) override;
      virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj, struct req_state* s,
          rgw::notify::EventType event_type, const string* object_name=nullptr) override;
      virtual RGWLC* get_rgwlc(void) override { return NULL; }
      virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }

      virtual int log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
      virtual int log_op(const DoutPrefixProvider *dpp, string& oid, bufferlist& bl) override;
      virtual int register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
          const map<string, string>& meta) override;
      virtual void get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota) override;
      virtual int set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled) override;
      virtual uint64_t get_new_req_id() override { return 0; }
      virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
          std::optional<rgw_zone_id> zone,
          std::optional<rgw_bucket> bucket,
          RGWBucketSyncPolicyHandlerRef *phandler,
          optional_yield y) override;
      virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
      virtual void wakeup_meta_sync_shards(set<int>& shard_ids) override { return; }
      virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, map<int, set<string> >& shard_ids) override { return; }
      virtual int clear_usage(const DoutPrefixProvider *dpp) override { return 0; }
      virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
          uint32_t max_entries, bool *is_truncated,
          RGWUsageIter& usage_iter,
          map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
      virtual int get_config_key_val(string name, bufferlist* bl) override;
      virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const string& section, const string& marker, void** phandle) override;
      virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<string>& keys, bool* truncated) override;
      virtual void meta_list_keys_complete(void* handle) override;
      virtual string meta_get_marker(void *handle) override;
      virtual int meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y) override;

      virtual const RGWSyncModuleInstanceRef& get_sync_module() { return sync_module; }
      virtual string get_host_id() { return ""; }

      virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() override;
      virtual std::unique_ptr<RGWRole> get_role(string name,
          string tenant,
          string path="",
          string trust_policy="",
          string max_session_duration_str="",
          std::multimap<string,string> tags={}) override;
      virtual std::unique_ptr<RGWRole> get_role(string id) override;
      virtual int get_roles(const DoutPrefixProvider *dpp,
          optional_yield y,
          const string& path_prefix,
          const string& tenant,
          vector<std::unique_ptr<RGWRole>>& roles) override;
      virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
      virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
          const string& tenant,
          vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
      virtual std::unique_ptr<MultipartUpload> get_multipart_upload(Bucket* bucket, const string& oid, std::optional<string> upload_id, ceph::real_time mtime) override;
      virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  const string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
      virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const string& unique_tag) override;

      virtual void finalize(void) override;

      virtual CephContext *ctx(void) override {
        return cctx;
      }

      virtual const string& get_luarocks_path() const override {
        return luarocks_path;
      }

      virtual void set_luarocks_path(const string& path) override {
        luarocks_path = path;
      }

      int open_idx(struct m0_uint128 *id, bool create, struct m0_idx *out);
      void close_idx(struct m0_idx *idx) { m0_idx_fini(idx); }
      int do_idx_op(struct m0_idx *, enum m0_idx_opcode opcode,
		    vector<uint8_t>& key, vector<uint8_t>& val, bool update = false);

      int do_idx_next_op(struct m0_idx *idx,
                         vector<vector<uint8_t>>& key_vec,
                         vector<vector<uint8_t>>& val_vec);
      int next_query_by_name(string idx_name,
                             vector<string>& key_str_vec, std::vector<bufferlist>& val_bl_vec);

      void index_name_to_motr_fid(string iname, struct m0_uint128 *fid);
      int open_motr_idx(struct m0_uint128 *id, struct m0_idx *idx);
      int create_motr_idx_by_name(string iname);
      int do_idx_op_by_name(string idx_name, enum m0_idx_opcode opcode,
                            string key_str, bufferlist &bl);
      int check_n_create_global_indices();
  };

} } // namespace rgw::sal
