// Network-layer helpers: find the laptop's IP address on the local Wi-Fi network.

const os = require('os');

/**
 * Returns the laptop's IPv4 address on the LAN (e.g. "192.168.1.5"),
 * which phones on the same Wi-Fi use to reach the server.
 * Skips loopback (127.0.0.1) and virtual adapters (VirtualBox, WSL, VPNs, ...).
 */
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  const candidates = [];

  for (const [name, addresses] of Object.entries(interfaces)) {
    for (const addr of addresses) {
      if (addr.family !== 'IPv4' || addr.internal) continue;
      if (/virtual|vmware|vbox|wsl|hyper-v|vethernet|loopback/i.test(name)) continue;
      candidates.push({ name, address: addr.address });
    }
  }

  // Prefer the Wi-Fi adapter when several are found.
  const wifi = candidates.find(c => /wi-?fi|wlan|wireless/i.test(c.name));
  return (wifi || candidates[0] || { address: '127.0.0.1' }).address;
}

module.exports = { getLocalIP };
