import { Routes } from '@angular/router';
import { ComparisonPageComponent } from './comparison-page.component';
import { SimulationPageComponent } from './simulation-page.component';

export const routes: Routes = [
	{ path: '', pathMatch: 'full', redirectTo: 'simulation' },
	{ path: 'simulation', component: SimulationPageComponent },
	{ path: 'comparison', component: ComparisonPageComponent },
	{ path: '**', redirectTo: 'simulation' },
];
