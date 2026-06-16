const form = document.getElementById('denunciaForm');
        const btn = document.getElementById('submitBtn');
        const toast = document.getElementById('toast');

        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            
            // UI Loading state
            btn.classList.add('loading');
            btn.disabled = true;

            const payload = {
                departamento: document.getElementById('departamento').value,
                provincia: document.getElementById('provincia').value,
                distrito: document.getElementById('distrito').value,
                tipo_hecho: document.getElementById('tipo_hecho').value
            };

            try {
                const res = await fetch('/api/denunciar', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                
                if (data.status === 'success') {
                    // Show success
                    form.reset();
                    toast.classList.add('show');
                    setTimeout(() => toast.classList.remove('show'), 3500);
                } else {
                    alert('Hubo un error al procesar su solicitud.');
                }
            } catch (err) {
                alert('Error de conexión. Asegúrese que el backend está corriendo.');
            } finally {
                btn.classList.remove('loading');
                btn.disabled = false;
            }
        });

        // Lógica de Select en cascada para UBIGEO
        const ubigeoText = document.getElementById('ubigeo-data').textContent;
        const ubigeoData = JSON.parse(ubigeoText);
        
        const selDept = document.getElementById('departamento');
        const selProv = document.getElementById('provincia');
        const selDist = document.getElementById('distrito');

        // Llenar departamentos ordenados
        Object.keys(ubigeoData).sort().forEach(dept => {
            const opt = document.createElement('option');
            opt.value = dept;
            opt.textContent = dept;
            selDept.appendChild(opt);
        });

        selDept.addEventListener('change', (e) => {
            selProv.innerHTML = '<option value="" disabled selected>Seleccione Provincia...</option>';
            selDist.innerHTML = '<option value="" disabled selected>Seleccione Distrito...</option>';
            selProv.disabled = false;
            selDist.disabled = true;
            
            const provincias = ubigeoData[e.target.value] || {};
            Object.keys(provincias).sort().forEach(prov => {
                const opt = document.createElement('option');
                opt.value = prov;
                opt.textContent = prov;
                selProv.appendChild(opt);
            });
        });

        selProv.addEventListener('change', (e) => {
            selDist.innerHTML = '<option value="" disabled selected>Seleccione Distrito...</option>';
            selDist.disabled = false;
            
            const dept = selDept.value;
            const prov = e.target.value;
            const distritos = ubigeoData[dept][prov] || [];
            
            distritos.forEach(dist => {
                const opt = document.createElement('option');
                opt.value = dist;
                opt.textContent = dist;
                selDist.appendChild(opt);
            });
        });
