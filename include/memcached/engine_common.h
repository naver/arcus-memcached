#ifndef ENGINE_COMMON_H
#define ENGINE_COMMON_H

/* Slab sizing definitions. */
#define POWER_SMALLEST   1
#define POWER_LARGEST    200
#define MAX_SLAB_CLASSES (POWER_LARGEST+1)

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct engine_interface {
        uint64_t interface; /**< The version number on the engine structure */
    } ENGINE_HANDLE;

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_COMMON_H */
