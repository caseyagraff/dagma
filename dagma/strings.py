STR_COMPUTE_NODE_REPR = "Comp(%s, %s, deps=%s, bound=%s)"

# === Error Messages ===
STR_MISSING_VAR_DEPS = "Not all graph variables are bound. Missing %s."

STR_NO_SAVE_FUNC = "No save function provided."

STR_SAVE_FUNC_EXCEPTION = 'Exception during save "%s"'
STR_LOAD_FUNC_EXCEPTION = 'Exception during load "%s"'

STR_SAVE_NOT_COMPUTED = "Node has not been computed. Cannot save."

STR_FILE_PATH_WRONG_TYPE_FOREACH = (
    'Incorrect type for file_path ("%s"). '
    + "Must be None or a function; which is required when using foreach."
)

STR_TRANSFORM_CHANGED = 'Transform function used for "%s" may have changed.'

STR_CHECKSUM_CHANGED = 'Checksum for "%s" doesn\'t match last save.'
