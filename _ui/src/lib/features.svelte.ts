import { api, type Features } from './api'

// Which optional parts of the console are switched on, from /info. Until the
// answer arrives everything reads as off, so a disabled button never flashes
// in; loaded tells a page whether "off" is real yet.
const off: Features = {
  security: false,
  create_topic: false,
  delete_topic: false,
  alter_topic: false,
  delete_records: false,
  delete_group: false,
  alter_broker: false,
}

let current = $state<Features>(off)
let loaded = $state(false)
let label = $state('')
let version = $state('')

api
  .info()
  .then((r) => {
    current = { ...off, ...r.features }
    label = r.info
    version = r.version ?? ''
    loaded = true
  })
  // Without /info the optional pages stay off, but stop waiting for it.
  .catch(() => (loaded = true))

export const app = {
  get features() {
    return current
  },
  get loaded() {
    return loaded
  },
  /** Short environment label from the config, e.g. "PreProd". */
  get info() {
    return label
  },
  /** Server build version, e.g. "v1.4.0". */
  get version() {
    return version
  },
}
