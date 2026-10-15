export default {
  __version: '7.2.5',
  'hydrator-Runner-full-false': function(isPrimaryKey, isEntity, isScalarReference, Collection, Reference, PolymorphicRef, ValidationError) {
    // compiled hydrator for entity Runner ( normalized)
    return function(entity, data, factory, newEntity, convertCustomTypes, schema, parentSchema, normalizeAccessors) {
      if (data.id === null) {
        entity.id = null;
      } else if (typeof data.id !== 'undefined') {
        entity.id = data.id;
      }
      if (data.name === null) {
        entity.name = null;
      } else if (typeof data.name !== 'undefined') {
        entity.name = data.name;
      }
      if (data.lastSeenAt === null) {
        entity.lastSeenAt = null;
      } else if (typeof data.lastSeenAt !== 'undefined') {
        if (data.lastSeenAt instanceof Date) {
          entity.lastSeenAt = data.lastSeenAt;
        } else if (typeof data.lastSeenAt === 'number' || data.lastSeenAt.includes('+') || data.lastSeenAt.lastIndexOf('-') > 10 || data.lastSeenAt.endsWith('Z')) {
          entity.lastSeenAt = new Date(data.lastSeenAt);
        } else {
          entity.lastSeenAt = new Date(data.lastSeenAt + 'Z');
        }
      }
    }
  },
  'hydrator-Runner-full-true': function(isPrimaryKey, isEntity, isScalarReference, Collection, Reference, PolymorphicRef, ValidationError) {
    // compiled hydrator for entity Runner ( normalized)
    return function(entity, data, factory, newEntity, convertCustomTypes, schema, parentSchema, normalizeAccessors) {
      if (data.id === null) {
        entity.id = null;
      } else if (typeof data.id !== 'undefined') {
        entity.id = data.id;
      }
      if (data.name === null) {
        entity.name = null;
      } else if (typeof data.name !== 'undefined') {
        entity.name = data.name;
      }
      if (data.lastSeenAt === null) {
        entity.lastSeenAt = null;
      } else if (typeof data.lastSeenAt !== 'undefined') {
        if (data.lastSeenAt instanceof Date) {
          entity.lastSeenAt = data.lastSeenAt;
        } else if (typeof data.lastSeenAt === 'number' || data.lastSeenAt.includes('+') || data.lastSeenAt.lastIndexOf('-') > 10 || data.lastSeenAt.endsWith('Z')) {
          entity.lastSeenAt = new Date(data.lastSeenAt);
        } else {
          entity.lastSeenAt = new Date(data.lastSeenAt + 'Z');
        }
      }
    }
  },
  'comparator-Runner': function(compareArrays, compareBooleans, compareBuffers, compareObjects, equals) {
    // compiled comparator for entity Runner
    return function(last, current, options) {
      const diff = {};
      if (current.id === null && last.id === undefined) {
        diff.id = current.id;
      } else if (current.id == null && last.id == null) {
    
      } else if ((current.id != null && last.id == null) || (current.id == null && last.id != null)) {
        diff.id = current.id;
      } else if (last.id !== current.id) {
        diff.id = current.id;
      }
    
      if (current.name === null && last.name === undefined) {
        diff.name = current.name;
      } else if (current.name == null && last.name == null) {
    
      } else if ((current.name != null && last.name == null) || (current.name == null && last.name != null)) {
        diff.name = current.name;
      } else if (last.name !== current.name) {
        diff.name = current.name;
      }
    
      if (current.lastSeenAt === null && last.lastSeenAt === undefined) {
        diff.lastSeenAt = current.lastSeenAt;
      } else if (current.lastSeenAt == null && last.lastSeenAt == null) {
    
      } else if ((current.lastSeenAt != null && last.lastSeenAt == null) || (current.lastSeenAt == null && last.lastSeenAt != null)) {
        diff.lastSeenAt = current.lastSeenAt;
      } else if (last.lastSeenAt.valueOf() !== current.lastSeenAt.valueOf()) {
        diff.lastSeenAt = current.lastSeenAt;
      }
    
    if (options?.includeInverseSides) {
    }
      return diff;
    }
  },
  'snapshotGenerator-Runner': function(clone, cloneEmbeddable, processDateProperty) {
    return function(entity) {
      const ret = {};
      if (typeof entity.id !== 'undefined') {
        ret.id = entity.id;
      }
    
      if (typeof entity.name !== 'undefined') {
        ret.name = entity.name;
      }
    
      if (typeof entity.lastSeenAt !== 'undefined') {
        ret.lastSeenAt = clone(processDateProperty(entity.lastSeenAt));
      }
    
      return ret;
    }
  },
  'resultMapper-Runner': function(PolymorphicRef, parseDate) {
    // compiled mapper for entity Runner
    return function(result) {
      const ret = {};
      const mapped = {};
      if (typeof result.id !== 'undefined') {
        ret.id = result.id;
        mapped.id = true;
      }
      if (typeof result.name !== 'undefined') {
        ret.name = result.name;
        mapped.name = true;
      }
      if (typeof result.last_seen_at !== 'undefined') {
        if (result.last_seen_at == null || result.last_seen_at instanceof Date) {
          ret.lastSeenAt = result.last_seen_at;
        } else if (typeof result.last_seen_at === 'bigint') {
          ret.lastSeenAt = parseDate(Number(result.last_seen_at));
        } else if (typeof result.last_seen_at === 'number' || result.last_seen_at.includes('+') || result.last_seen_at.lastIndexOf('-') > 10 || result.last_seen_at.endsWith('Z')) {
          ret.lastSeenAt = parseDate(result.last_seen_at);
        } else {
          ret.lastSeenAt = parseDate(result.last_seen_at + 'Z');
        }
        mapped.last_seen_at = true;
      }
      for (let k in result) { if (Object.hasOwn(result, k) && !mapped[k] && ret[k] === undefined) ret[k] = result[k]; }
      return ret;
    }
  },
  'hydrator-Runner-reference-false': function(isPrimaryKey, isEntity, isScalarReference, Collection, Reference, PolymorphicRef, ValidationError) {
    // compiled hydrator for entity Runner ( normalized)
    return function(entity, data, factory, newEntity, convertCustomTypes, schema, parentSchema, normalizeAccessors) {
      if (data.id === null) {
        entity.id = null;
      } else if (typeof data.id !== 'undefined') {
        entity.id = data.id;
      }
    }
  },
  'hydrator-Runner-reference-true': function(isPrimaryKey, isEntity, isScalarReference, Collection, Reference, PolymorphicRef, ValidationError) {
    // compiled hydrator for entity Runner ( normalized)
    return function(entity, data, factory, newEntity, convertCustomTypes, schema, parentSchema, normalizeAccessors) {
      if (data.id === null) {
        entity.id = null;
      } else if (typeof data.id !== 'undefined') {
        entity.id = data.id;
      }
    }
  },
  'pkGetter-Runner': function(isEntityOrRef) {
    // compiled pk getter for entity Runner
    return function(entity) {
      return entity.id;
    }
  },
  'pkGetterConverted-Runner': function(isEntityOrRef) {
    // compiled pk getter (with converted custom types) for entity Runner
    return function(entity) {
      return entity.id;
    }
  },
  'pkSerializer-Runner': function(isEntityOrRef, getCompositeKeyValue, getPrimaryKeyHash) {
    // compiled pk serializer for entity Runner
    return function(entity) {
      return '' + entity.id;
    }
  }
};
