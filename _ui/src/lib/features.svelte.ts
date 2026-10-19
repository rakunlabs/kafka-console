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

api
  .info()
  .then((r) => {
    current = { ...off, ...r.features }
    label = r.info
    loaded = true
  })
  .catch(() => {})

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
}
